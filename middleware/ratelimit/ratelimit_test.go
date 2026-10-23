package ratelimit_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex/errorx"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func ok(context.Context, any) (any, error) { return "ok", nil }

// keyLimiter allows only the listed keys and records the keys it sees.
type keyLimiter struct {
	mu    sync.Mutex
	seen  []string
	allow map[string]bool
}

func (l *keyLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, key)
	return l.allow[key]
}

func TestMiddlewareRejects(t *testing.T) {
	l := &keyLimiter{allow: map[string]bool{"": false}}
	called := false
	_, err := ratelimit.New(l)(func(context.Context, any) (any, error) { called = true; return nil, nil })(t.Context(), nil)
	e, isErrorx := errors.AsType[*errorx.Error](err)
	if !isErrorx || e.Code != errorx.ResourceExhausted || e.Reason != ratelimit.ReasonRateLimited {
		t.Fatalf("err = %v", err)
	}
	if !errorx.IsRetryable(err) {
		t.Fatal("rate limit error should be retryable")
	}
	if called {
		t.Fatal("next endpoint called")
	}
	if len(l.seen) != 1 || l.seen[0] != "" {
		t.Fatalf("default key = %q, want global empty key", l.seen)
	}
}

func TestMiddlewareKey(t *testing.T) {
	l := &keyLimiter{allow: map[string]bool{"alice": true}}
	mw := ratelimit.New(l, nil, ratelimit.WithKey(nil), ratelimit.WithKey(func(_ context.Context, req any) string {
		return req.(string)
	}))
	if resp, err := mw(ok)(t.Context(), "alice"); err != nil || resp != "ok" {
		t.Fatalf("alice: (%v, %v)", resp, err)
	}
	if _, err := mw(ok)(t.Context(), "bob"); errorx.CodeOf(err) != errorx.ResourceExhausted {
		t.Fatalf("bob: %v", err)
	}
}

func TestTokenBucketBurstAndRefill(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	l := ratelimit.TokenBucket(2, 3, ratelimit.WithClock(c.Now))

	for i := range 3 {
		if !l.Allow("k") {
			t.Fatalf("call %d rejected within burst", i)
		}
	}
	if l.Allow("k") {
		t.Fatal("call beyond burst allowed")
	}
	c.Advance(250 * time.Millisecond) // half a token
	if l.Allow("k") {
		t.Fatal("allowed with half a token")
	}
	c.Advance(250 * time.Millisecond) // one token
	if !l.Allow("k") {
		t.Fatal("rejected after refilling one token")
	}
	if l.Allow("k") {
		t.Fatal("allowed twice after refilling one token")
	}
	c.Advance(time.Hour) // refill caps at burst
	allowed := 0
	for range 10 {
		if l.Allow("k") {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d after a long idle period, want burst 3", allowed)
	}
}

func TestTokenBucketPerKey(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	l := ratelimit.TokenBucket(1, 1, ratelimit.WithClock(c.Now))
	if !l.Allow("a") || l.Allow("a") {
		t.Fatal("bucket a misbehaves")
	}
	if !l.Allow("b") {
		t.Fatal("bucket b shares state with a")
	}
}

func TestTokenBucketClockGoingBackwards(t *testing.T) {
	c := &clock{now: time.Unix(100, 0)}
	l := ratelimit.TokenBucket(1, 1, ratelimit.WithClock(c.Now))
	l.Allow("k")
	c.Advance(-time.Hour)
	if l.Allow("k") {
		t.Fatal("a clock step backwards refilled the bucket")
	}
}

func TestTokenBucketSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := ratelimit.TokenBucket(10, 1) // real (fake-in-bubble) time.Now
		mw := ratelimit.New(l)(ok)
		var allowed atomic.Int64
		for range 10 {
			if _, err := mw(t.Context(), nil); err == nil {
				allowed.Add(1)
			}
			time.Sleep(50 * time.Millisecond) // half a token per iteration
		}
		// t=0 uses the initial token; refills at t=100ms, 200ms, ... 400ms.
		if got := allowed.Load(); got != 5 {
			t.Fatalf("allowed = %d, want 5", got)
		}
	})
}

func TestTokenBucketConcurrent(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	l := ratelimit.TokenBucket(1, 50, ratelimit.WithClock(c.Now))
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			if l.Allow("shared") {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed = %d, want 50", got)
	}
}

func TestTokenBucketPanics(t *testing.T) {
	for name, fn := range map[string]func(){
		"zero rate":  func() { ratelimit.TokenBucket(0, 1) },
		"nan rate":   func() { ratelimit.TokenBucket(math.NaN(), 1) },
		"inf rate":   func() { ratelimit.TokenBucket(math.Inf(1), 1) },
		"zero burst": func() { ratelimit.TokenBucket(1, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			fn()
		})
	}
}
