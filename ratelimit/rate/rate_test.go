package rate_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mwratelimit "github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex-contrib/ratelimit/rate"
	"github.com/nostosx/servex/errorx"
)

var _ mwratelimit.Limiter = (*rate.Limiter)(nil)

func TestBurstRefillAndKeyIsolation(t *testing.T) {
	now := time.Unix(1000, 0)
	l, err := rate.New(2, 2, rate.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if !l.Allow("a") {
			t.Fatal("initial burst was not available")
		}
	}
	if l.Allow("a") || !l.Allow("b") {
		t.Fatal("burst or independent key quota is incorrect")
	}
	now = now.Add(250 * time.Millisecond)
	if l.Allow("a") {
		t.Fatal("partial token was admitted")
	}
	now = now.Add(250 * time.Millisecond)
	if !l.Allow("a") || l.Allow("a") {
		t.Fatal("rejection consumed future quota or refill was incorrect")
	}
}

func TestKeyBoundDoesNotResetSpentQuota(t *testing.T) {
	now := time.Unix(1000, 0)
	l, err := rate.New(0.001, 1, rate.WithMaxKeys(1), rate.WithIdleTimeout(time.Second), rate.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if !l.Allow("existing") || l.Allow("new") {
		t.Fatal("key bound was not enforced")
	}
	now = now.Add(time.Minute)
	if l.Allow("new") || l.Allow("existing") {
		t.Fatal("idle eviction restored quota before the bucket refilled")
	}
	now = now.Add(1000 * time.Second)
	if !l.Allow("new") || l.Allow("existing") {
		t.Fatal("fully replenished idle key was not safely reclaimed")
	}
}

func TestMiddlewareComposition(t *testing.T) {
	now := time.Unix(1000, 0)
	l, err := rate.New(1, 1, rate.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ep := mwratelimit.New(l, mwratelimit.WithKey(func(_ context.Context, req any) string { return req.(string) }))(func(context.Context, any) (any, error) {
		calls++
		return "ok", nil
	})
	if resp, err := ep(t.Context(), "tenant-a"); resp != "ok" || err != nil {
		t.Fatalf("first call = %v, %v", resp, err)
	}
	if _, err := ep(t.Context(), "tenant-a"); errorx.CodeOf(err) != errorx.ResourceExhausted {
		t.Fatalf("rejection = %v", err)
	}
	if _, err := ep(t.Context(), "tenant-b"); err != nil || calls != 2 {
		t.Fatalf("independent tenant = %v, calls = %d", err, calls)
	}
}

func TestConcurrentBurst(t *testing.T) {
	l, err := rate.New(1, 7, rate.WithClock(func() time.Time { return time.Unix(1000, 0) }))
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if l.Allow("shared") {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != 7 {
		t.Fatalf("admitted %d calls, want exactly 7", got)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name  string
		limit float64
		burst int
		opts  []rate.Option
	}{
		{name: "zero rate", burst: 1},
		{name: "negative rate", limit: -1, burst: 1},
		{name: "NaN", limit: math.NaN(), burst: 1},
		{name: "infinity", limit: math.Inf(1), burst: 1},
		{name: "zero burst", limit: 1},
		{name: "negative burst", limit: 1, burst: -1},
		{name: "zero key bound", limit: 1, burst: 1, opts: []rate.Option{rate.WithMaxKeys(0)}},
		{name: "zero idle timeout", limit: 1, burst: 1, opts: []rate.Option{rate.WithIdleTimeout(0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, err := rate.New(tt.limit, tt.burst, tt.opts...)
			if e, ok := errors.AsType[*errorx.Error](err); l != nil || !ok || e.Code != errorx.InvalidArgument {
				t.Fatalf("New = %v, %v", l, err)
			}
		})
	}
}

func TestZeroValueFailsClosed(t *testing.T) {
	var zero rate.Limiter
	var nilLimiter *rate.Limiter
	if zero.Allow("key") || nilLimiter.Allow("key") {
		t.Fatal("uninitialized limiter admitted a call")
	}
}
