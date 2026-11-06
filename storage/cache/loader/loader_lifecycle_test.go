package loader

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/kitx/singleflight"

	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/storage/cache"
)

func TestNewLoaderRequiresPositiveTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		l, err := newLoader[int](t.Context(), newFakeCache(), timeout)
		if l != nil || !errors.Is(err, ErrInvalidLoadTimeout) {
			t.Fatalf("NewLoader(%v) = %v, %v", timeout, l, err)
		}
	}
}

func TestLoaderUsesOwnerValuesAndScopedKeys(t *testing.T) {
	type scopeKey struct{}
	owner := context.WithValue(t.Context(), scopeKey{}, "service")
	l, err := newLoader[string](owner, newFakeCache(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, scope := range []string{"tenant-a", "tenant-b"} {
		waiter := context.WithValue(t.Context(), scopeKey{}, scope)
		got, err := l.Get(waiter, scope+":user", func(ctx context.Context) (string, error) {
			if ctx.Value(scopeKey{}) != "service" {
				t.Error("shared execution inherited waiter credentials")
			}
			return scope, nil
		})
		if err != nil || got != scope {
			t.Fatalf("scoped Get = %q, %v; want %q", got, err, scope)
		}
	}
}

func TestLoaderTimeoutRejectsLateSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeCache()
		l, err := newLoader[int](t.Context(), c, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		start := time.Now()
		got, err := l.Get(t.Context(), "key", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 42, nil // A late success must not enter the cache.
		})
		if got != 0 || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Fatalf("timeout Get = %d, %v after %v", got, err, time.Since(start))
		}
		if c.setCount() != 0 {
			t.Fatal("late result was written back")
		}
		got, err = l.Get(t.Context(), "key", func(context.Context) (int, error) { return 7, nil })
		if got != 7 || err != nil {
			t.Fatalf("retry after timed-out execution = %d, %v", got, err)
		}
	})
}

func TestLoaderTimeoutDuringEncodingSkipsWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "encoded"},
		{name: "encoding failed", err: errors.New("cannot encode")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newFakeCache()
				codec := marshalCodec{Codec: testJSONCodec{}, marshal: func(v any) ([]byte, error) {
					time.Sleep(2 * time.Second) // A synchronous codec can outlive the timeout.
					if tc.err != nil {
						return nil, tc.err
					}
					return testJSONCodec{}.Marshal(v)
				}}
				l, err := newLoader[int](t.Context(), c, time.Second, WithCodec(codec))
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				got, err := l.Get(t.Context(), "key", func(context.Context) (int, error) { return 42, nil })
				if got != 0 || !errors.Is(err, context.DeadlineExceeded) || c.setCount() != 0 {
					t.Fatalf("timed-out encoding: value=%d, err=%v, writes=%d", got, err, c.setCount())
				}
			})
		})
	}
}

type marshalCodec struct {
	encoding.Codec
	marshal func(any) ([]byte, error)
}

func (c marshalCodec) Marshal(v any) ([]byte, error) { return c.marshal(v) }

func TestLoaderOwnerCancellationStopsLoadsAndHits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		c := newFakeCache()
		c.data["hit"] = []byte("7")
		l, err := newLoader[int](owner, c, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		cause := errors.New("service stopped")
		done := make(chan error, 1)
		go func() {
			_, err := l.Get(t.Context(), "miss", func(ctx context.Context) (int, error) {
				<-ctx.Done()
				return 42, context.Cause(ctx)
			})
			done <- err
		}()
		synctest.Wait()
		cancel(cause)
		if err := <-done; !errors.Is(err, cause) {
			t.Fatalf("running load = %v", err)
		}
		for _, key := range []string{"hit", "new"} {
			if _, err := l.Get(t.Context(), key, nil); !errors.Is(err, cause) {
				t.Fatalf("Get(%q) after owner cancellation = %v", key, err)
			}
		}
		if c.setCount() != 0 {
			t.Fatal("canceled execution was cached")
		}
	})
}

func TestLoaderCloseCancelsAndJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newTestLoader[int](t, newFakeCache())
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		canceled := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			_, err := l.Get(t.Context(), "key", func(ctx context.Context) (int, error) {
				<-ctx.Done()
				close(canceled)
				<-release
				return 42, nil
			})
			result <- err
		}()
		synctest.Wait()
		closed := make(chan struct{})
		go func() { _ = l.Close(); close(closed) }()
		<-canceled
		select {
		case <-closed:
			t.Fatal("Close returned before the callback joined")
		default:
		}
		if _, err := l.Get(t.Context(), "new", nil); !errors.Is(err, ErrLoaderClosed) {
			t.Fatalf("admission during Close = %v", err)
		}
		unblock()
		<-closed
		if err := <-result; !errors.Is(err, ErrLoaderClosed) {
			t.Fatalf("closed execution = %v", err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLoaderConcurrentCloseAndAdmission(t *testing.T) {
	l := newTestLoader[int](t, newFakeCache())
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			<-start
			if i%4 == 0 {
				_ = l.Close()
				return
			}
			_, err := l.Get(t.Context(), "key", func(ctx context.Context) (int, error) {
				<-ctx.Done()
				return 0, context.Cause(ctx)
			})
			if !errors.Is(err, ErrLoaderClosed) {
				t.Errorf("racing Get = %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
}

func TestLoaderRejectsCanceledWaiterOnHit(t *testing.T) {
	c := newFakeCache()
	c.data["key"] = []byte("42")
	l := newTestLoader[int](t, c)
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("request canceled")
	cancel(cause)
	if got, err := l.Get(ctx, "key", nil); got != 0 || !errors.Is(err, cause) {
		t.Fatalf("canceled cache hit = %d, %v", got, err)
	}
	if _, err := l.Get(t.Context(), "missing", nil); !errors.Is(err, singleflight.ErrInvalidFunc) {
		t.Fatalf("nil load on miss = %v", err)
	}
}

func TestLoaderRejectsCancellationDuringLookup(t *testing.T) {
	c := newFakeCache()
	c.data["key"] = []byte("42")
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	cause := errors.New("canceled during cache lookup")
	canceling := lookupCache{Cache: c, get: func(ctx context.Context, key string) ([]byte, error) {
		value, err := c.Get(ctx, key)
		cancel(cause)
		return value, err
	}}
	l := newTestLoader[int](t, canceling)
	if got, err := l.Get(ctx, "key", nil); got != 0 || !errors.Is(err, cause) {
		t.Fatalf("cancellation raced with hit: value=%d, err=%v", got, err)
	}
}

func TestLoaderCloseRejectsHitsWithoutClosingBackend(t *testing.T) {
	c := newFakeCache()
	c.data["key"] = []byte("42")
	l := newTestLoader[int](t, c)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Get(t.Context(), "key", nil); !errors.Is(err, ErrLoaderClosed) {
		t.Fatalf("closed Loader returned a cache hit: %v", err)
	}
	if value, err := c.Get(t.Context(), "key"); string(value) != "42" || err != nil {
		t.Fatalf("Close affected the backing cache: %q, %v", value, err)
	}
}

func TestLoaderGoexitSettlesWaiters(t *testing.T) {
	l := newTestLoader[int](t, newFakeCache())
	if _, err := l.Get(t.Context(), "key", func(context.Context) (int, error) {
		runtime.Goexit()
		return 0, nil
	}); !errors.Is(err, singleflight.ErrGoexit) {
		t.Fatalf("Goexit Get = %v", err)
	}
}

func TestLoaderZeroValueIsClosed(t *testing.T) {
	var l Loader[int]
	if _, err := l.Get(t.Context(), "key", nil); !errors.Is(err, ErrLoaderClosed) {
		t.Fatalf("zero Loader Get = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

type lookupCache struct {
	cache.Cache
	get func(context.Context, string) ([]byte, error)
}

func (c lookupCache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.get(ctx, key)
}

func TestLoaderRechecksCacheAfterDelayedMiss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeCache()
		firstMiss := make(chan struct{})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var reads, loads atomic.Int32
		delayed := lookupCache{Cache: c, get: func(ctx context.Context, key string) ([]byte, error) {
			if reads.Add(1) == 1 {
				close(firstMiss)
				<-release
				return nil, cache.ErrNotFound
			}
			return c.Get(ctx, key)
		}}
		l := newTestLoader[int](t, delayed)
		done := make(chan error, 1)
		load := counter(&loads, 42, nil)
		go func() { _, err := l.Get(t.Context(), "key", load); done <- err }()
		<-firstMiss
		if _, err := l.Get(t.Context(), "key", load); err != nil {
			t.Fatal(err)
		}
		unblock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if loads.Load() != 1 || c.setCount() != 1 {
			t.Fatalf("delayed miss reloaded: loads=%d, writes=%d", loads.Load(), c.setCount())
		}
	})
}

func TestLoaderBackendFailureAtEitherLookupSkipsWrite(t *testing.T) {
	for _, failedRead := range []int32{1, 2} {
		c := newFakeCache()
		var reads, loads atomic.Int32
		flaky := lookupCache{Cache: c, get: func(ctx context.Context, key string) ([]byte, error) {
			if reads.Add(1) == failedRead {
				return nil, errors.New("backend unavailable")
			}
			return c.Get(ctx, key)
		}}
		l := newTestLoader[int](t, flaky)
		got, err := l.Get(t.Context(), "key", counter(&loads, 42, nil))
		if got != 42 || err != nil || loads.Load() != 1 || c.setCount() != 0 {
			t.Fatalf("failure at lookup %d: value=%d, err=%v, loads=%d, writes=%d",
				failedRead, got, err, loads.Load(), c.setCount())
		}
	}
}
