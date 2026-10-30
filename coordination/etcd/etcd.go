// Package etcd provides lease-backed locks and elections using etcd's official
// concurrency package. A lease is not permission to write after ownership loss.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Coordinator owns no connection or background process. Each acquired Lock or
// Leadership owns one keepalive session and must be released. The caller owns
// the SDK client. Its context must live for the entire lock/leadership lifetime.
type Coordinator struct {
	client *clientv3.Client
	prefix string
	ttl    int
}

func New(client *clientv3.Client, prefix string, ttl int) (*Coordinator, error) {
	if client == nil || prefix == "" || !strings.HasSuffix(prefix, "/") || ttl < 1 || ttl > 86400 {
		return nil, errors.New("etcd: client, namespace ending in / and TTL of 1..86400 seconds required")
	}
	return &Coordinator{client: client, prefix: prefix, ttl: ttl}, nil
}

func (c *Coordinator) session(ctx context.Context) (*concurrency.Session, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return concurrency.NewSession(c.client, concurrency.WithTTL(c.ttl), concurrency.WithContext(ctx))
}

func (c *Coordinator) key(kind, name string) (string, error) {
	if name == "" || len(name) > 256 || strings.ContainsAny(name, "/\x00") {
		return "", errors.New("etcd: name must be a nonempty path segment of at most 256 bytes")
	}
	return c.prefix + kind + "/" + name, nil
}

// Lock exposes Done and a monotonically increasing acquisition revision. Enforce
// that revision in the protected resource as a fencing token; watching Done alone
// cannot prevent a paused former owner from executing a late external write.
type Lock struct {
	lease    lease
	mutex    *concurrency.Mutex
	Revision int64
}

func (c *Coordinator) Acquire(ctx context.Context, name string) (*Lock, error) {
	key, err := c.key("locks", name)
	if err != nil {
		return nil, err
	}
	session, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	mutex := concurrency.NewMutex(session, key)
	if err := mutex.Lock(ctx); err != nil {
		c.cleanup(session)
		return nil, err
	}
	owner, err := c.client.Get(ctx, mutex.Key())
	if err != nil {
		c.cleanup(session)
		return nil, err
	}
	if len(owner.Kvs) != 1 {
		c.cleanup(session)
		return nil, concurrency.ErrSessionExpired
	}
	select {
	case <-session.Done():
		c.cleanup(session)
		return nil, concurrency.ErrSessionExpired
	default:
	}
	return &Lock{lease: lease{client: c.client, session: session}, mutex: mutex, Revision: owner.Kvs[0].CreateRevision}, nil
}

func (l *Lock) Done() <-chan struct{} { return l.lease.session.Done() }

// Release is idempotent. Cancellation can prevent immediate server-side revoke;
// keepalive is still stopped and the lease then expires according to its TTL.
func (l *Lock) Release(ctx context.Context) error {
	return l.lease.release(ctx, l.mutex.Unlock)
}

// Leadership is obtained by Campaign and retained while its session is alive.
// Revision can fence external writes when enforced by the protected resource.
type Leadership struct {
	lease    lease
	election *concurrency.Election
	Revision int64
}

func (c *Coordinator) Campaign(ctx context.Context, name, value string) (*Leadership, error) {
	key, err := c.key("elections", name)
	if err != nil {
		return nil, err
	}
	if len(value) > 4096 {
		return nil, fmt.Errorf("etcd: election value exceeds limit")
	}
	session, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	election := concurrency.NewElection(session, key)
	if err := election.Campaign(ctx, value); err != nil {
		c.cleanup(session)
		return nil, err
	}
	select {
	case <-session.Done():
		c.cleanup(session)
		return nil, concurrency.ErrSessionExpired
	default:
	}
	return &Leadership{lease: lease{client: c.client, session: session}, election: election, Revision: election.Rev()}, nil
}

func (l *Leadership) Done() <-chan struct{} { return l.lease.session.Done() }
func (l *Leadership) Resign(ctx context.Context) error {
	return l.lease.release(ctx, l.election.Resign)
}

type lease struct {
	client  *clientv3.Client
	session *concurrency.Session
	once    sync.Once
	err     error
}

func (l *lease) release(ctx context.Context, unlock func(context.Context) error) error {
	l.once.Do(func() {
		l.err = unlock(ctx)
		l.session.Orphan()
		_, err := l.client.Revoke(ctx, l.session.Lease())
		l.err = errors.Join(l.err, err)
	})
	return l.err
}

func (c *Coordinator) cleanup(session *concurrency.Session) {
	session.Orphan()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.client.Revoke(ctx, session.Lease())
}
