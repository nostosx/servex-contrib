package loader

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/storage/cache"
)

// fakeCache is an in-memory Cache that records writes and can inject errors.
type fakeCache struct {
	mu      sync.Mutex
	data    map[string][]byte
	ttls    map[string]time.Duration
	sets    int
	getErr  error
	setErr  error
	waitGet chan struct{} // when non-nil, Get blocks until it is closed
}

func newFakeCache() *fakeCache {
	return &fakeCache{data: map[string][]byte{}, ttls: map[string]time.Duration{}}
}

func (c *fakeCache) Get(_ context.Context, key string) ([]byte, error) {
	if c.waitGet != nil {
		<-c.waitGet
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	v, ok := c.data[key]
	if !ok {
		return nil, cache.ErrNotFound
	}
	return v, nil
}

func (c *fakeCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	if c.setErr != nil {
		return c.setErr
	}
	c.data[key] = value
	c.ttls[key] = ttl
	return nil
}

func (c *fakeCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	return nil
}

func (c *fakeCache) entry(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[key]
	return string(v), ok
}

func (c *fakeCache) setCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sets
}

// decimalCodec encodes ints as decimal text.
type decimalCodec struct{}

func (decimalCodec) Name() string { return "decimal" }

func (decimalCodec) Marshal(v any) ([]byte, error) {
	return strconv.AppendInt(nil, int64(v.(int)), 10), nil
}

func (decimalCodec) Unmarshal(data []byte, v any) error {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	*v.(*int) = n
	return nil
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// counter returns a load function that counts its calls and returns v, err.
func counter[T any](calls *atomic.Int32, v T, err error) func(context.Context) (T, error) {
	return func(context.Context) (T, error) {
		calls.Add(1)
		return v, err
	}
}

func TestLoaderGet(t *testing.T) {
	errBackend := errors.New("backend down")
	errSet := errors.New("set failed")
	errLoad := errors.New("load failed")
	want := user{ID: 1, Name: "ada"}

	tests := []struct {
		name      string
		seed      string // cached bytes under "k"; "" means absent
		getErr    error
		setErr    error
		loadErr   error
		wantUser  user
		wantErr   error
		wantCalls int32
		wantEntry string // expected cached bytes afterwards; "" means absent
	}{
		{
			name:      "hit decodes without loading",
			seed:      `{"id":1,"name":"ada"}`,
			wantUser:  want,
			wantEntry: `{"id":1,"name":"ada"}`,
		},
		{
			name:      "miss loads and writes back",
			wantUser:  want,
			wantCalls: 1,
			wantEntry: `{"id":1,"name":"ada"}`,
		},
		{
			name:      "decode error is a miss and is overwritten",
			seed:      `not json`,
			wantUser:  want,
			wantCalls: 1,
			wantEntry: `{"id":1,"name":"ada"}`,
		},
		{
			name:      "write-back error is ignored",
			setErr:    errSet,
			wantUser:  want,
			wantCalls: 1,
		},
		{
			name:      "load error is returned and not cached",
			loadErr:   errLoad,
			wantErr:   errLoad,
			wantCalls: 1,
		},
		{
			name:      "other get error loads without write-back",
			getErr:    errBackend,
			wantUser:  want,
			wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeCache()
			if tt.seed != "" {
				c.data["k"] = []byte(tt.seed)
			}
			c.getErr, c.setErr = tt.getErr, tt.setErr
			l := newTestLoader[user](t, c)

			var calls atomic.Int32
			loaded := want
			if tt.loadErr != nil {
				loaded = user{}
			}
			got, err := l.Get(t.Context(), "k", counter(&calls, loaded, tt.loadErr))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.wantUser {
				t.Fatalf("got %+v, want %+v", got, tt.wantUser)
			}
			if n := calls.Load(); n != tt.wantCalls {
				t.Fatalf("load called %d times, want %d", n, tt.wantCalls)
			}
			c.getErr = nil
			entry, ok := c.entry("k")
			if tt.wantEntry == "" && ok {
				t.Fatalf("unexpected cache entry %q", entry)
			}
			if tt.wantEntry != "" && entry != tt.wantEntry {
				t.Fatalf("cache entry = %q, want %q", entry, tt.wantEntry)
			}
		})
	}
}

func TestLoaderOtherGetErrorSkipsWriteBack(t *testing.T) {
	c := newFakeCache()
	c.getErr = errors.New("backend down")
	l := newTestLoader[int](t, c)
	var calls atomic.Int32
	if _, err := l.Get(t.Context(), "k", counter(&calls, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if n := c.setCount(); n != 0 {
		t.Fatalf("Set called %d times, want 0", n)
	}
}

func TestOptions(t *testing.T) {
	c := newFakeCache()
	l := newTestLoader[int](t, c, WithCodec(decimalCodec{}), WithTTL(time.Minute), WithCodec(nil), nil)
	var calls atomic.Int32
	if v, err := l.Get(t.Context(), "n", counter(&calls, 42, nil)); v != 42 || err != nil {
		t.Fatalf("Get = %d, %v", v, err)
	}
	if entry, _ := c.entry("n"); entry != "42" {
		t.Fatalf("entry = %q, want decimal 42", entry)
	}
	if ttl := c.ttls["n"]; ttl != time.Minute {
		t.Fatalf("ttl = %v, want 1m", ttl)
	}
	// The second Get hits the cache and decodes with the same codec.
	if v, err := l.Get(t.Context(), "n", counter(&calls, 0, nil)); v != 42 || err != nil {
		t.Fatalf("Get = %d, %v", v, err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("load called %d times, want 1", n)
	}
}

func TestLoaderDefaultTTLNeverExpires(t *testing.T) {
	c := newFakeCache()
	l := newTestLoader[string](t, c)
	var calls atomic.Int32
	if _, err := l.Get(t.Context(), "k", counter(&calls, "v", nil)); err != nil {
		t.Fatal(err)
	}
	if ttl := c.ttls["k"]; ttl != 0 {
		t.Fatalf("ttl = %v, want 0", ttl)
	}
}

func TestLoaderSingleLoadPerKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 32
		c := newFakeCache()
		c.waitGet = make(chan struct{})
		l := newTestLoader[user](t, c)

		var (
			calls   atomic.Int32
			release = make(chan struct{})
			wg      sync.WaitGroup
		)
		load := func(context.Context) (user, error) {
			calls.Add(1)
			<-release
			return user{ID: 7}, nil
		}
		for range n {
			wg.Go(func() {
				got, err := l.Get(t.Context(), "k", load)
				if err != nil || got.ID != 7 {
					t.Errorf("Get = %+v, %v", got, err)
				}
			})
		}
		// Let every caller observe a miss before the first load can finish,
		// so that all of them reach the singleflight group.
		synctest.Wait()
		close(c.waitGet)
		synctest.Wait()
		close(release)
		wg.Wait()

		if got := calls.Load(); got != 1 {
			t.Fatalf("load called %d times, want 1", got)
		}
		if got := c.setCount(); got != 1 {
			t.Fatalf("Set called %d times, want 1", got)
		}
	})
}

func TestLoaderSharedLoadError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 8
		errLoad := errors.New("load failed")
		c := newFakeCache()
		c.waitGet = make(chan struct{})
		l := newTestLoader[int](t, c)

		var (
			calls   atomic.Int32
			release = make(chan struct{})
			wg      sync.WaitGroup
			failed  atomic.Int32
		)
		for range n {
			wg.Go(func() {
				_, err := l.Get(t.Context(), "k", func(context.Context) (int, error) {
					calls.Add(1)
					<-release
					return 0, errLoad
				})
				if errors.Is(err, errLoad) {
					failed.Add(1)
				}
			})
		}
		synctest.Wait()
		close(c.waitGet)
		synctest.Wait()
		close(release)
		wg.Wait()

		if calls.Load() != 1 || failed.Load() != n {
			t.Fatalf("calls=%d failed=%d, want 1 and %d", calls.Load(), failed.Load(), n)
		}
		if c.setCount() != 0 {
			t.Fatal("failed load was written back")
		}
	})
}

func TestLoaderDistinctKeysLoadIndependently(t *testing.T) {
	c := newFakeCache()
	l := newTestLoader[string](t, c)
	var calls atomic.Int32
	for _, k := range []string{"a", "b", "a", "b"} {
		v, err := l.Get(t.Context(), k, counter(&calls, k+"!", nil))
		if err != nil || v != k+"!" {
			t.Fatalf("Get(%q) = %q, %v", k, v, err)
		}
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("load called %d times, want 2", n)
	}
}

func TestNewLoaderNilCachePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewLoader(nil) did not panic")
		}
	}()
	_, _ = newLoader[int](t.Context(), nil, time.Minute)
}

func TestLoaderWaiterHonoursOwnContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newTestLoader[int](t, newFakeCache())
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		slow := func(context.Context) (int, error) { <-release; return 42, nil }

		leader := make(chan error, 1)
		go func() { _, err := l.Get(t.Context(), "k", slow); leader <- err }()
		synctest.Wait()

		// A follower with a short deadline must not wait for the slow load.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		begin := time.Now()
		_, err := l.Get(ctx, "k", slow)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(begin) != 10*time.Millisecond {
			t.Fatalf("follower Get() = %v after %v, want DeadlineExceeded after 10ms", err, time.Since(begin))
		}
		unblock()
		if err := <-leader; err != nil {
			t.Fatalf("leader Get() = %v", err)
		}
	})
}

func TestLoaderLeaderCancellationDoesNotFailFollowers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newTestLoader[int](t, newFakeCache())
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		load := func(ctx context.Context) (int, error) {
			<-release
			return 42, ctx.Err() // the shared load must not see the leader's cancellation
		}

		leaderCtx, cancelLeader := context.WithCancel(t.Context())
		defer cancelLeader()
		leader := make(chan error, 1)
		go func() { _, err := l.Get(leaderCtx, "k", load); leader <- err }()
		synctest.Wait()
		follower := make(chan int, 1)
		go func() {
			v, err := l.Get(t.Context(), "k", load)
			if err != nil {
				t.Errorf("follower Get() = %v", err)
			}
			follower <- v
		}()
		synctest.Wait()

		cancelLeader()
		if err := <-leader; !errors.Is(err, context.Canceled) {
			t.Fatalf("leader Get() = %v, want Canceled", err)
		}
		unblock()
		if v := <-follower; v != 42 {
			t.Fatalf("follower got %d, want 42", v)
		}
	})
}

func newTestLoader[T any](t *testing.T, c cache.Cache, opts ...Option) *Loader[T] {
	t.Helper()
	l, err := newLoader[T](t.Context(), c, time.Minute, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}
