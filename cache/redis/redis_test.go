package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/storage/cache"
)

// newTestCache returns a Cache over a fresh miniredis server.
func newTestCache(t *testing.T, opts ...Option) (*Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, opts...), mr
}

func TestNewNilClientPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(nil) did not panic")
		}
	}()
	New(nil)
}

func TestGetSetDelete(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := t.Context()

	if _, err := c.Get(ctx, "k"); err != cache.ErrNotFound {
		t.Fatalf("Get missing = %v, want cache.ErrNotFound", err)
	}
	if err := c.Set(ctx, "k", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "k")
	if err != nil || string(got) != "v1" {
		t.Fatalf("Get = %q, %v; want v1", got, err)
	}
	if err := c.Set(ctx, "k", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get(ctx, "k"); string(got) != "v2" {
		t.Fatalf("Get after overwrite = %q, want v2", got)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want cache.ErrNotFound", err)
	}
	if err := c.Delete(ctx, "absent"); err != nil {
		t.Fatalf("Delete absent = %v, want nil", err)
	}
}

func TestEmptyValue(t *testing.T) {
	c, _ := newTestCache(t)
	if err := c.Set(t.Context(), "k", nil, 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(t.Context(), "k")
	if err != nil || len(got) != 0 {
		t.Fatalf("Get = %q, %v; want empty value", got, err)
	}
}

func TestPrefix(t *testing.T) {
	c, mr := newTestCache(t, WithPrefix("app:"))
	ctx := t.Context()
	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if v, err := mr.Get("app:k"); err != nil || v != "v" {
		t.Fatalf("raw app:k = %q, %v; want v", v, err)
	}
	if mr.Exists("k") {
		t.Fatal("unprefixed key was written")
	}
	if err := mr.Set("k", "other"); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get(ctx, "k"); string(got) != "v" {
		t.Fatalf("Get = %q, want the prefixed value", got)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists("app:k") || !mr.Exists("k") {
		t.Fatal("Delete did not target the prefixed key only")
	}
}

func TestTTL(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := t.Context()

	if err := c.Set(ctx, "short", []byte("v"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("short"); ttl != 2*time.Second {
		t.Fatalf("TTL = %v, want 2s", ttl)
	}
	mr.FastForward(time.Second)
	if _, err := c.Get(ctx, "short"); err != nil {
		t.Fatalf("Get before expiry = %v", err)
	}
	mr.FastForward(time.Second)
	if _, err := c.Get(ctx, "short"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after expiry = %v, want cache.ErrNotFound", err)
	}
}

func TestTTLNonPositiveNeverExpires(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := t.Context()
	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
			t.Fatal(err)
		}
		// A non-positive ttl must also clear an existing expiry.
		if err := c.Set(ctx, "k", []byte("v"), ttl); err != nil {
			t.Fatal(err)
		}
		if got := mr.TTL("k"); got != 0 {
			t.Fatalf("ttl %v: TTL = %v, want none", ttl, got)
		}
		mr.FastForward(24 * time.Hour)
		if _, err := c.Get(ctx, "k"); err != nil {
			t.Fatalf("ttl %v: Get = %v, want hit", ttl, err)
		}
	}
}

func TestTTLSubMillisecondRoundsUp(t *testing.T) {
	c, mr := newTestCache(t)
	if err := c.Set(t.Context(), "k", []byte("v"), time.Microsecond); err != nil {
		t.Fatal(err)
	}
	if got := mr.TTL("k"); got != time.Millisecond {
		t.Fatalf("TTL = %v, want 1ms", got)
	}
}

func TestServerErrors(t *testing.T) {
	tests := []struct {
		reply     string
		code      errorx.Code
		reason    string
		retryable bool
	}{
		{"LOADING Redis is loading the dataset in memory", errorx.Unavailable, ReasonUnavailable, true},
		{"READONLY You can't write against a read only replica.", errorx.Unavailable, ReasonUnavailable, true},
		{"CLUSTERDOWN The cluster is down", errorx.Unavailable, ReasonUnavailable, true},
		{"ERR max number of clients reached", errorx.Unavailable, ReasonUnavailable, true},
		{"WRONGTYPE Operation against a key holding the wrong kind of value", errorx.Internal, ReasonServerError, false},
	}
	for _, tt := range tests {
		t.Run(tt.reply, func(t *testing.T) {
			c, mr := newTestCache(t)
			mr.SetError(tt.reply)
			ops := map[string]func() error{
				"get":    func() error { _, err := c.Get(t.Context(), "k"); return err },
				"set":    func() error { return c.Set(t.Context(), "k", []byte("v"), 0) },
				"delete": func() error { return c.Delete(t.Context(), "k") },
			}
			for name, op := range ops {
				err := op()
				e, ok := errors.AsType[*errorx.Error](err)
				if !ok {
					t.Fatalf("%s: error %v is not an *errorx.Error", name, err)
				}
				if e.Code != tt.code || e.Reason != tt.reason || e.Retryable() != tt.retryable {
					t.Fatalf("%s: got code=%v reason=%s retryable=%v", name, e.Code, e.Reason, e.Retryable())
				}
				if _, ok := errors.AsType[goredis.Error](err); !ok {
					t.Fatalf("%s: cause lost: %v", name, err)
				}
				if errors.Is(err, cache.ErrNotFound) {
					t.Fatalf("%s: server error reported as a miss", name)
				}
			}
		})
	}
}

func TestWrongTypeReply(t *testing.T) {
	c, mr := newTestCache(t)
	if _, err := mr.Lpush("k", "x"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Get(t.Context(), "k")
	if errorx.CodeOf(err) != errorx.Internal || errorx.IsRetryable(err) {
		t.Fatalf("Get on a list = %v, want non-retryable Internal", err)
	}
}

func TestConnectionFailureIsUnavailable(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{
		Addr:        mr.Addr(),
		MaxRetries:  -1,
		DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })
	c := New(client)
	mr.Close()

	ops := map[string]func() error{
		"get":    func() error { _, err := c.Get(t.Context(), "k"); return err },
		"set":    func() error { return c.Set(t.Context(), "k", []byte("v"), 0) },
		"delete": func() error { return c.Delete(t.Context(), "k") },
	}
	for name, op := range ops {
		err := op()
		e, ok := errors.AsType[*errorx.Error](err)
		if !ok || e.Code != errorx.Unavailable || e.Reason != ReasonUnavailable || !e.Retryable() {
			t.Fatalf("%s: error = %v, want retryable Unavailable", name, err)
		}
		if e.Unwrap() == nil {
			t.Fatalf("%s: cause lost", name)
		}
	}
}

func TestClosedClient(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	c := New(client)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := c.Get(t.Context(), "k")
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.FailedPrecondition || e.Reason != ReasonClosed || e.Retryable() {
		t.Fatalf("Get on closed client = %v, want non-retryable Unavailable", err)
	}
	if !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("cause lost: %v", err)
	}
}

func TestContextErrorsAreKept(t *testing.T) {
	c, _ := newTestCache(t)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := c.Get(canceled, "k")
	if !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
		t.Fatalf("Get with canceled ctx = %v (code %v), want Canceled", err, errorx.CodeOf(err))
	}

	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	err = c.Set(expired, "k", []byte("v"), 0)
	if !errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) != errorx.DeadlineExceeded {
		t.Fatalf("Set with expired ctx = %v (code %v), want DeadlineExceeded", err, errorx.CodeOf(err))
	}
}

func TestUniversalClient(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { _ = client.Close() })
	c := New(client, WithPrefix("u:"))
	if err := c.Set(t.Context(), "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(t.Context(), "k"); err != nil || string(got) != "v" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestLoader(t *testing.T) {
	c, mr := newTestCache(t, WithPrefix("users:"))
	l, err := cache.NewLoader[user](t.Context(), c, time.Minute, cache.WithTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	var calls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})
	var releaseOnce, startedOnce sync.Once
	unlock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unlock()
	load := func(context.Context) (user, error) {
		calls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return user{ID: 1, Name: "ada"}, nil
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([]user, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { results[i], errs[i] = l.Get(t.Context(), "1", load) })
	}
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("loader did not start")
	}
	unlock()
	wg.Wait()

	for i := range n {
		if errs[i] != nil || results[i] != (user{ID: 1, Name: "ada"}) {
			t.Fatalf("caller %d: %+v, %v", i, results[i], errs[i])
		}
	}
	if got := calls.Load(); got < 1 || got > n {
		t.Fatalf("load calls = %d", got)
	}

	raw, err := mr.Get("users:1")
	if err != nil || raw != `{"id":1,"name":"ada"}` {
		t.Fatalf("written back %q, %v", raw, err)
	}
	if ttl := mr.TTL("users:1"); ttl != time.Minute {
		t.Fatalf("write-back TTL = %v, want 1m", ttl)
	}

	// A hit is served from Redis without loading.
	before := calls.Load()
	got, err := l.Get(t.Context(), "1", func(context.Context) (user, error) {
		calls.Add(1)
		return user{}, errors.New("unexpected load")
	})
	if err != nil || got.Name != "ada" || calls.Load() != before {
		t.Fatalf("hit: %+v, %v, loads %d", got, err, calls.Load()-before)
	}

	// A corrupt value is treated as a miss and overwritten.
	if err := mr.Set("users:1", "not json"); err != nil {
		t.Fatal(err)
	}
	got, err = l.Get(t.Context(), "1", func(context.Context) (user, error) { return user{ID: 1, Name: "bob"}, nil })
	if err != nil || got.Name != "bob" {
		t.Fatalf("reload: %+v, %v", got, err)
	}
	if raw, _ := mr.Get("users:1"); raw != `{"id":1,"name":"bob"}` {
		t.Fatalf("overwritten value %q", raw)
	}
}

func TestLoaderDegradesWhenRedisFails(t *testing.T) {
	c, mr := newTestCache(t)
	l, err := cache.NewLoader[string](t.Context(), c, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	mr.SetError("LOADING Redis is loading the dataset in memory")

	got, err := l.Get(t.Context(), "k", func(context.Context) (string, error) { return "direct", nil })
	if err != nil || got != "direct" {
		t.Fatalf("Get = %q, %v; want direct load", got, err)
	}
	mr.SetError("")
	if mr.Exists("k") {
		t.Fatal("value written back to a failing cache")
	}
}
