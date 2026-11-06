package loader

import (
	"context"
	"errors"
	"time"

	"github.com/nostosx/kitx/singleflight"

	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/storage/cache"
)

// ErrLoaderClosed reports a closed or uninitialized Loader.
var ErrLoaderClosed = singleflight.ErrClosed

// ErrCodecRequired reports missing codec configuration.
var ErrCodecRequired = errors.New("cache: WithCodec is required")

// ErrInvalidLoadTimeout reports a non-positive shared execution timeout.
var ErrInvalidLoadTimeout = singleflight.ErrInvalidTimeout

// Option configures a Loader.
type Option func(*loaderOptions)

type loaderOptions struct {
	codec encoding.Codec
	ttl   time.Duration
}

// WithCodec sets the codec used to encode values written to and decode values
// read from the cache. A non-nil codec must be supplied explicitly.
func WithCodec(c encoding.Codec) Option {
	return func(o *loaderOptions) {
		if c != nil {
			o.codec = c
		}
	}
}

// WithTTL sets the ttl of entries written back by the Loader. The default is
// 0, which means entries never expire.
func WithTTL(d time.Duration) Option {
	return func(o *loaderOptions) { o.ttl = d }
}

// Loader provides read-through access to a cache.Cache for values of type T: it
// decodes hits, and on a miss loads the value once per key across concurrent
// callers and writes it back. A Loader is safe for concurrent use when its
// codec is; load callbacks for different keys may run concurrently. Create it
// with New and call Close to join shared work. The zero value is closed.
// A Loader must not be copied after first use.
type Loader[T any] struct {
	cache  cache.Cache
	codec  encoding.Codec
	ttl    time.Duration
	group  *singleflight.SharedGroup[string, T]
	owner  context.Context
	cancel context.CancelCauseFunc
}

// New returns a Loader backed by c, owned by ctx. Shared executions
// inherit ctx's values and cancellation, with a positive per-execution timeout.
// It returns ErrInvalidLoadTimeout for a non-positive timeout and panics if c
// or ctx is nil. WithCodec is required; an absent codec returns
// ErrCodecRequired. Call Close even if ctx is canceled to join outstanding work.
func New[T any](ctx context.Context, c cache.Cache, timeout time.Duration, opts ...Option) (*Loader[T], error) {
	if c == nil {
		panic("cache: NewLoader with nil Cache")
	}
	if ctx == nil {
		panic("cache: NewLoader with nil Context")
	}
	var o loaderOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if timeout <= 0 {
		return nil, ErrInvalidLoadTimeout
	}
	if o.codec == nil {
		return nil, ErrCodecRequired
	}
	owner, cancel := context.WithCancelCause(ctx)
	group, err := singleflight.NewShared[string, T](owner, timeout)
	if err != nil {
		cancel(err)
		return nil, err
	}
	return &Loader[T]{cache: c, codec: o.codec, ttl: o.ttl, group: group, owner: owner, cancel: cancel}, nil
}

// Get returns the value for key.
//
// On a hit the cached bytes are decoded and returned. A value that fails to
// decode is treated as a miss and is overwritten by the reloaded value.
//
// On a miss (cache.ErrNotFound) load is called, but only once at a time per key:
// concurrent callers for the same key share that call and its result,
// including its error. The shared call inherits the constructor's owner context,
// never a waiter's values or cancellation. Each caller may stop waiting with its
// own cancellation cause. The execution timeout covers cache rechecking, loading,
// encoding and write-back. Backends and callbacks must honor cancellation;
// synchronous codec calls cannot be interrupted. Load and encoding results
// observed after cancellation are not written back. Cancellation cannot undo
// a write already in progress.
// Encoding and write-back failures do not affect a successful load result.
// Load errors are returned as is and nothing is cached. A nil load is rejected
// on a miss with singleflight.ErrInvalidFunc.
//
// Any other error from the cache (for example an unreachable backend) also
// falls through to load, but the loaded value is not written back. Both the
// initial lookup and the shared execution's recheck must permit write-back.
//
// Values shared between concurrent callers are the same T value; when T
// contains pointers, slices or maps, callers must not mutate the result.
// Concurrent loads for the same key must be interchangeable: only the callback
// of the caller that starts the shared execution is used. Include tenant and
// authorization scope in keys, and supply shared credentials through the owner
// context or callback dependencies. Distinct active keys require caller-managed
// admission limits. A callback panic terminates the process as in an ordinary
// goroutine; Goexit settles waiters with singleflight.ErrGoexit.
func (l *Loader[T]) Get(ctx context.Context, key string, load func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := l.cause(ctx); err != nil {
		return zero, err
	}
	v, hit, writeBack := l.read(ctx, key)
	if err := l.cause(ctx); err != nil {
		return zero, err
	}
	if hit {
		return v, nil
	}
	if load == nil {
		return zero, singleflight.ErrInvalidFunc
	}
	return l.group.Do(ctx, key, func(loadCtx context.Context) (T, error) {
		// Another execution may have filled the cache since the initial miss.
		v, hit, canWrite := l.read(loadCtx, key)
		if err := context.Cause(loadCtx); err != nil {
			return zero, err
		}
		if hit {
			return v, nil
		}
		v, err := load(loadCtx)
		if cause := context.Cause(loadCtx); cause != nil {
			return zero, cause
		}
		if err != nil || !writeBack || !canWrite {
			return v, err
		}
		if data, merr := l.codec.Marshal(v); merr == nil {
			if cause := context.Cause(loadCtx); cause != nil {
				return zero, cause
			}
			_ = l.cache.Set(loadCtx, key, data, l.ttl)
		}
		if cause := context.Cause(loadCtx); cause != nil {
			return zero, cause
		}
		return v, nil
	})
}

func (l *Loader[T]) read(ctx context.Context, key string) (value T, hit, writeBack bool) {
	data, err := l.cache.Get(ctx, key)
	if err == nil && l.codec.Unmarshal(data, &value) == nil {
		return value, true, true
	}
	return value, false, err == nil || errors.Is(err, cache.ErrNotFound)
}

func (l *Loader[T]) cause(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if l.owner == nil {
		return ErrLoaderClosed
	}
	return context.Cause(l.owner)
}

// Close stops admission, cancels shared executions and waits for them to return.
// Concurrent calls are safe and idempotent. It does not close the backing Cache.
// Callbacks must honor cancellation; calling Close or reentering the same key
// from a load callback deadlocks and is prohibited.
func (l *Loader[T]) Close() error {
	if l.cancel == nil {
		return nil
	}
	l.cancel(ErrLoaderClosed)
	return l.group.Close()
}
