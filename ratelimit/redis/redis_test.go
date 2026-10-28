package redis_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	redislimit "github.com/nostosx/servex-contrib/ratelimit/redis"
	"github.com/nostosx/servex/errorx"
)

func fixture(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	s.SetTime(time.Unix(100, 0))
	c := goredis.NewClient(&goredis.Options{Addr: s.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = c.Close() })
	return s, c
}

func TestSharedBucket(t *testing.T) {
	t.Parallel()
	s, c := fixture(t)
	first, err := redislimit.New(c, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := redislimit.New(c, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []*redislimit.Limiter{first, second} {
		if allowed, _, err := l.AllowContext(t.Context(), "alice"); err != nil || !allowed {
			t.Fatalf("burst: allowed=%v err=%v", allowed, err)
		}
	}
	if allowed, delay, err := second.AllowContext(t.Context(), "alice"); err != nil || allowed || delay != 500*time.Millisecond {
		t.Fatalf("shared rejection: allowed=%v delay=%v err=%v", allowed, delay, err)
	}
	if allowed, _, err := first.AllowContext(t.Context(), "bob"); err != nil || !allowed {
		t.Fatalf("key isolation: allowed=%v err=%v", allowed, err)
	}
	s.SetTime(time.Unix(100, 500_000_000))
	if allowed, _, err := first.AllowContext(t.Context(), "alice"); err != nil || !allowed {
		t.Fatalf("refill: allowed=%v err=%v", allowed, err)
	}
	separate, err := redislimit.New(c, 2, 2, redislimit.WithPrefix("other:"))
	if err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := separate.AllowContext(t.Context(), "alice"); err != nil || !allowed {
		t.Fatalf("namespace isolation: allowed=%v err=%v", allowed, err)
	}
	s.FastForward(2 * time.Second)
	if len(s.Keys()) != 0 {
		t.Fatal("idle buckets did not expire")
	}
}

func TestConcurrentReplicas(t *testing.T) {
	t.Parallel()
	_, c := fixture(t)
	l, err := redislimit.New(c, 1, 12)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 60 {
		wg.Go(func() {
			ok, _, err := l.AllowContext(t.Context(), "shared")
			if err != nil {
				t.Error(err)
			}
			if ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if allowed.Load() != 12 {
		t.Fatalf("granted %d, want 12", allowed.Load())
	}
}

func TestFailures(t *testing.T) {
	t.Parallel()
	_, c := fixture(t)
	for name, values := range map[string]struct {
		rate  float64
		burst int
	}{
		"zero": {0, 1}, "nan": {math.NaN(), 1}, "infinite": {math.Inf(1), 1}, "burst": {1, 0}, "overflow": {1e-300, 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := redislimit.New(c, values.rate, values.burst); errorx.CodeOf(err) != errorx.InvalidArgument {
				t.Fatalf("configuration: %v", err)
			}
		})
	}
	l, err := redislimit.New(c, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := l.AllowContext(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.AllowContext(t.Context(), "key"); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatalf("closed: %v", err)
	}
}
