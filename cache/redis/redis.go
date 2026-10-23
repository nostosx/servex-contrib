package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/storage/cache"
)

// Reasons attached to the errorx errors returned by Cache.
const (
	// ReasonUnavailable marks failures to reach Redis or transient server
	// conditions (loading, failover, cluster down, client limits).
	ReasonUnavailable = "REDIS_UNAVAILABLE"
	// ReasonClosed marks operations on a closed client. They are reported as
	// errorx.FailedPrecondition and are not retryable: the client will never
	// recover.
	ReasonClosed = "REDIS_CLIENT_CLOSED"
	// ReasonServerError marks error replies from Redis that retrying does not
	// fix, such as WRONGTYPE.
	ReasonServerError = "REDIS_SERVER_ERROR"
)

// Option configures a Cache.
type Option func(*options)

type options struct {
	prefix string
}

// WithPrefix prepends s to every key, which namespaces the entries of one
// Cache inside a shared Redis database. The default is no prefix.
func WithPrefix(s string) Option {
	return func(o *options) { o.prefix = s }
}

// Cache is a cache.Cache stored in Redis. Values are stored as plain Redis
// strings under the prefixed key. Cache does not own the client: closing the
// client is the caller's responsibility. All methods are safe for concurrent
// use.
type Cache struct {
	client goredis.UniversalClient
	prefix string
}

var _ cache.Cache = (*Cache)(nil)

// New returns a Cache backed by client. It panics if client is nil.
func New(client goredis.UniversalClient, opts ...Option) *Cache {
	if client == nil {
		panic("redis: New with nil client")
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Cache{client: client, prefix: o.prefix}
}

// Get returns the value stored under key, or cache.ErrNotFound when the key
// is absent or expired.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := c.client.Get(ctx, c.prefix+key).Bytes()
	switch {
	case err == nil:
		return b, nil
	case errors.Is(err, goredis.Nil):
		return nil, cache.ErrNotFound
	default:
		return nil, classify(ctx, "get", err)
	}
}

// Set stores value under key. A ttl <= 0 stores the entry without expiry,
// replacing any expiry the key had; a positive ttl below one millisecond is
// rounded up to one millisecond, the resolution of Redis.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	switch {
	case ttl <= 0:
		ttl = 0 // go-redis: zero means no expiry
	case ttl < time.Millisecond:
		ttl = time.Millisecond
	}
	if err := c.client.Set(ctx, c.prefix+key, value, ttl).Err(); err != nil {
		return classify(ctx, "set", err)
	}
	return nil
}

// Delete removes key. Deleting an absent key is not an error.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, c.prefix+key).Err(); err != nil {
		return classify(ctx, "delete", err)
	}
	return nil
}

// transientPrefixes are Redis error replies describing a temporary server
// condition, mirroring the replies go-redis itself retries.
var transientPrefixes = []string{
	"LOADING ",
	"READONLY ",
	"CLUSTERDOWN ",
	"TRYAGAIN ",
	"MASTERDOWN ",
	"NOREPLICAS ",
	"ERR max number of clients reached",
}

// classify converts a go-redis error from operation op into an error suitable
// for callers, keeping err as the cause:
//
//   - a failure caused by ctx ending keeps the context error, so that
//     errorx.From reports Canceled or DeadlineExceeded;
//   - a closed client is errorx.FailedPrecondition, not retryable;
//   - an error reply from Redis is errorx.Unavailable (retryable) for
//     transient conditions and errorx.Internal otherwise;
//   - anything else (dial, I/O, timeouts, pool exhaustion) is
//     errorx.Unavailable and retryable.
func classify(ctx context.Context, op string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return fmt.Errorf("redis: %s: %w", op, err)
	}
	if errors.Is(err, goredis.ErrClosed) {
		return errorx.New(errorx.FailedPrecondition, ReasonClosed, "redis: "+op+": client is closed").
			WithCause(err)
	}
	if _, ok := errors.AsType[goredis.Error](err); ok {
		msg := err.Error()
		for _, p := range transientPrefixes {
			if strings.HasPrefix(msg, p) {
				return errorx.New(errorx.Unavailable, ReasonUnavailable, "redis: "+op+": server unavailable").
					WithCause(err)
			}
		}
		return errorx.New(errorx.Internal, ReasonServerError, "redis: "+op+": server error").WithCause(err)
	}
	return errorx.New(errorx.Unavailable, ReasonUnavailable, "redis: "+op+": connection failed").WithCause(err)
}
