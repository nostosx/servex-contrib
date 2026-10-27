package etcd

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// Defaults for Registry options.
const (
	defaultNamespace = "/servex/services"
	defaultTTL       = 15 * time.Second
)

// Error reasons used by this package.
const (
	reasonInvalid     = "INVALID_INSTANCE"
	reasonUnavailable = "REGISTRY_UNAVAILABLE"
)

// Compile-time interface checks.
var (
	_ discovery.Registrar  = (*Registry)(nil)
	_ discovery.Discoverer = (*Registry)(nil)
)

// Option configures a Registry.
type Option func(*options)

// options holds the configuration collected from Options.
type options struct {
	namespace string
	ttl       time.Duration
}

// WithNamespace sets the key prefix under which instances are stored. The
// default is "/servex/services". Trailing slashes are removed; an empty
// namespace keeps the default.
func WithNamespace(ns string) Option {
	return func(o *options) {
		if ns = strings.TrimRight(ns, "/"); ns != "" {
			o.namespace = ns
		}
	}
}

// WithTTL sets the lease TTL of registered instances. It is rounded up to
// whole seconds, the granularity of etcd leases. The default is 15s;
// non-positive values keep the default.
func WithTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.ttl = d
		}
	}
}

// Registry is an etcd backed service registry. It implements both
// discovery.Registrar and discovery.Discoverer. The client is owned by the
// caller and is not closed by the Registry. All methods are safe for
// concurrent use.
type Registry struct {
	client    *clientv3.Client
	namespace string
	ttl       int64 // seconds

	// mu serializes Register and Deregister and guards keepers.
	mu      sync.Mutex
	keepers map[string]*keeper // by key
}

// New returns a Registry that uses client.
func New(client *clientv3.Client, opts ...Option) *Registry {
	o := options{namespace: defaultNamespace, ttl: defaultTTL}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Registry{
		client:    client,
		namespace: o.namespace,
		ttl:       max(1, int64(math.Ceil(o.ttl.Seconds()))),
		keepers:   make(map[string]*keeper),
	}
}

// Register stores ins under <namespace>/<name>/<id>, attached to a new
// lease, replacing any instance with the same Name and ID. The lease is kept
// alive in the background until Deregister; when it is lost, for example
// because it expired during a network partition, the instance is written
// again with a new lease.
func (r *Registry) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	val, err := encode(ins)
	if err != nil {
		return errorx.New(errorx.InvalidArgument, reasonInvalid, "etcd: encode instance").WithCause(err)
	}
	key := r.key(ins.Name, ins.ID)

	r.mu.Lock()
	defer r.mu.Unlock()
	// Stop the keeper of a previous registration first so that it cannot
	// write the key concurrently with a lease that is revoked below.
	old, replacing := r.keepers[key]
	if replacing {
		old.stop()
	}
	kctx, cancel := context.WithCancel(context.Background())
	lease, ch, err := r.grantAndPut(ctx, kctx, key, val)
	if err != nil {
		cancel()
		if replacing {
			// Keep the previous registration alive.
			r.resume(key, old)
		}
		return backendErr(fmt.Sprintf("register %q", key), err)
	}
	if replacing {
		// The key now belongs to the new lease, so revoking the old lease
		// does not delete it.
		_, _ = r.client.Revoke(ctx, old.leaseID())
	}
	r.start(kctx, cancel, key, val, lease, ch)
	return nil
}

// start records and runs a keeper for key.
func (r *Registry) start(ctx context.Context, cancel context.CancelFunc, key, val string, lease clientv3.LeaseID, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	k := &keeper{cancel: cancel, done: make(chan struct{}), val: val, lease: lease}
	r.keepers[key] = k
	go k.run(ctx, r, key, ch)
}

// resume restarts a stopped keeper on its last lease. When the lease cannot
// be kept alive any more the new keeper registers the instance again.
func (r *Registry) resume(key string, k *keeper) {
	ctx, cancel := context.WithCancel(context.Background())
	lease := k.leaseID()
	ch, err := r.client.KeepAlive(ctx, lease)
	if err != nil {
		closed := make(chan *clientv3.LeaseKeepAliveResponse)
		close(closed)
		ch = closed
	}
	r.start(ctx, cancel, key, k.val, lease, ch)
}

// Deregister removes the instance with the Name and ID of ins: it stops the
// keep-alive, revokes the lease and deletes the key. Removing an unknown
// instance is not an error.
func (r *Registry) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := r.key(ins.Name, ins.ID)

	r.mu.Lock()
	defer r.mu.Unlock()
	if k, ok := r.keepers[key]; ok {
		delete(r.keepers, key)
		k.stop()
		// A failed revoke is not fatal: the key is deleted below, and the
		// lease expires on its own now that it is no longer kept alive.
		_, _ = r.client.Revoke(ctx, k.leaseID())
	}
	// The key may belong to another process or to a lease that could not be
	// revoked; deleting it makes the removal authoritative.
	if _, err := r.client.Delete(ctx, key); err != nil {
		return backendErr(fmt.Sprintf("deregister %q", key), err)
	}
	return nil
}

// grantAndPut grants a lease and writes key with it using ctx, then starts
// keeping the lease alive until kctx is done.
func (r *Registry) grantAndPut(ctx, kctx context.Context, key, val string) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	g, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return 0, nil, err
	}
	if _, err := r.client.Put(ctx, key, val, clientv3.WithLease(g.ID)); err != nil {
		// Best effort: an unrevoked lease expires after its TTL.
		_, _ = r.client.Revoke(ctx, g.ID)
		return 0, nil, err
	}
	ch, err := r.client.KeepAlive(kctx, g.ID)
	if err != nil {
		_, _ = r.client.Revoke(ctx, g.ID)
		return 0, nil, err
	}
	return g.ID, ch, nil
}

// keeper keeps the lease of one registered instance alive.
type keeper struct {
	cancel context.CancelFunc
	done   chan struct{}
	val    string // the encoded instance

	mu    sync.Mutex
	lease clientv3.LeaseID
}

// run drains the keep-alive channel and registers the instance again with a
// new lease whenever the channel closes before ctx is done.
func (k *keeper) run(ctx context.Context, r *Registry, key string, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	defer close(k.done)
	for {
		for range ch {
		}
		if ctx.Err() != nil {
			return
		}
		// The lease expired or was revoked behind our back.
		delay := minRetry
		for {
			lease, next, err := r.grantAndPut(ctx, ctx, key, k.val)
			if err == nil {
				k.mu.Lock()
				k.lease = lease
				k.mu.Unlock()
				ch = next
				break
			}
			if !sleep(ctx, delay) {
				return
			}
			delay = min(2*delay, maxRetry)
		}
	}
}

// stop stops the keeper and waits for it to exit.
func (k *keeper) stop() {
	k.cancel()
	<-k.done
}

// leaseID returns the current lease of the keeper.
func (k *keeper) leaseID() clientv3.LeaseID {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lease
}

// Watch returns a Watcher for the service name. It reads the current
// instances bounded by ctx and then follows changes with an etcd watch until
// the Watcher is stopped. Interrupted watches, including compactions, are
// recovered by reading the full list again.
func (r *Registry) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := r.namespace + "/" + name + "/"
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, backendErr(fmt.Sprintf("watch %q", name), err)
	}
	st := newState()
	items := load(name, resp.Kvs)
	st.set(values(items))

	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := newWatcher(st, cancel)
	go func() {
		defer close(w.done)
		r.follow(bg, name, prefix, resp.Header.Revision, items, st)
	}()
	return w, nil
}

// follow applies watch events after revision rev to items and publishes the
// result to st until ctx is done.
func (r *Registry) follow(ctx context.Context, name, prefix string, rev int64, items map[string]*discovery.Instance, st *state) {
	delay := minRetry
	for {
		wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		for wr := range r.client.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1)) {
			if wr.Err() != nil {
				break
			}
			for _, ev := range wr.Events {
				key := string(ev.Kv.Key)
				switch ev.Type {
				case mvccpb.PUT:
					if ins, ok := decode(name, ev.Kv.Key, ev.Kv.Value); ok {
						items[key] = ins
					} else {
						delete(items, key)
					}
				case mvccpb.DELETE:
					delete(items, key)
				}
			}
			st.set(values(items))
			delay = minRetry
		}
		cancel()

		// The watch ended: resynchronize with a full read.
		for {
			if !sleep(ctx, delay) {
				return
			}
			delay = min(2*delay, maxRetry)
			resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
			if err != nil {
				continue
			}
			items = load(name, resp.Kvs)
			rev = resp.Header.Revision
			st.set(values(items))
			break
		}
	}
}

// key returns the etcd key of an instance.
func (r *Registry) key(name, id string) string {
	return r.namespace + "/" + name + "/" + id
}

// record is the JSON representation of an instance stored in etcd.
type record struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Version   string            `json:"version,omitzero"`
	Weight    int               `json:"weight,omitzero"`
	Metadata  map[string]string `json:"metadata,omitzero"`
	Endpoints []string          `json:"endpoints,omitzero"`
}

// encode returns the JSON representation of ins.
func encode(ins *discovery.Instance) (string, error) {
	b, err := json.Marshal(record{
		ID:        ins.ID,
		Name:      ins.Name,
		Version:   ins.Version,
		Weight:    ins.Weight,
		Metadata:  ins.Metadata,
		Endpoints: ins.Endpoints,
	})
	return string(b), err
}

// decode parses a stored instance of service name. Values that are not
// valid instances are reported as not ok.
func decode(name string, key, val []byte) (*discovery.Instance, bool) {
	var rec record
	if err := json.Unmarshal(val, &rec); err != nil {
		return nil, false
	}
	if rec.ID == "" {
		rec.ID = string(key[strings.LastIndexByte(string(key), '/')+1:])
	}
	return &discovery.Instance{
		ID:        rec.ID,
		Name:      name,
		Version:   rec.Version,
		Weight:    rec.Weight,
		Metadata:  rec.Metadata,
		Endpoints: rec.Endpoints,
	}, true
}

// load decodes the instances in kvs by key.
func load(name string, kvs []*mvccpb.KeyValue) map[string]*discovery.Instance {
	items := make(map[string]*discovery.Instance, len(kvs))
	for _, kv := range kvs {
		if ins, ok := decode(name, kv.Key, kv.Value); ok {
			items[string(kv.Key)] = ins
		}
	}
	return items
}

// values returns the instances of items as a new slice.
func values(items map[string]*discovery.Instance) []*discovery.Instance {
	out := make([]*discovery.Instance, 0, len(items))
	for _, ins := range items {
		out = append(out, ins)
	}
	return out
}

// validate checks that ins identifies an instance.
func validate(ins *discovery.Instance) error {
	switch {
	case ins == nil:
		return errorx.New(errorx.InvalidArgument, reasonInvalid, "etcd: nil instance")
	case ins.Name == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "etcd: instance %q has an empty name", ins.ID)
	case ins.ID == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "etcd: instance of %q has an empty id", ins.Name)
	}
	return validateName(ins.Name)
}

// validateName checks that name can be used as a key segment.
func validateName(name string) error {
	if name == "" || strings.Contains(name, "/") {
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "etcd: invalid service name %q", name)
	}
	return nil
}

// backendErr classifies a failed etcd call. Context errors are kept as they
// are; everything else is reported as a retryable errorx.Unavailable whose
// cause is err.
func backendErr(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("etcd: %s: %w", op, err)
	}
	return errorx.New(errorx.Unavailable, reasonUnavailable, "etcd: "+op).WithCause(err)
}
