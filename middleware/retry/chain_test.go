package retry_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex-contrib/middleware/retry"
	"github.com/nostosx/servex-contrib/middleware/timeout"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// fakeBackend is an endpoint whose behaviour can be switched between hanging
// until the attempt deadline, failing fast and succeeding.
type fakeBackend struct {
	mu    sync.Mutex
	mode  string // "hang", "fail" or "ok"
	calls int
}

func (b *fakeBackend) set(mode string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mode = mode
}

func (b *fakeBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *fakeBackend) endpoint(ctx context.Context, req any) (any, error) {
	b.mu.Lock()
	b.calls++
	mode := b.mode
	b.mu.Unlock()
	switch mode {
	case "hang":
		<-ctx.Done()
		return nil, ctx.Err()
	case "fail":
		return nil, errorx.New(errorx.Unavailable, "DOWN", "backend down")
	default:
		return req, nil
	}
}

// clientChain builds the recommended client chain around be.
func clientChain(be *fakeBackend) endpoint.Endpoint {
	return endpoint.Chain(
		retry.New(retry.WithMaxAttempts(3), retry.WithBackoff(func(int) time.Duration { return 100 * time.Millisecond })),
		circuitbreaker.New(circuitbreaker.WithMinRequests(6), circuitbreaker.WithRandom(func() float64 { return 0 })),
		timeout.New(time.Second),
	)(be.endpoint)
}

func TestClientChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		be := &fakeBackend{mode: "hang"}
		ep := clientChain(be)
		ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "/svc/Get"})

		// Each attempt times out after 1s and is retried after 100ms.
		start := time.Now()
		_, err := ep(ctx, "req")
		if elapsed := time.Since(start); elapsed != 3*time.Second+200*time.Millisecond {
			t.Errorf("elapsed = %v, want 3.2s for 3 timed-out attempts", elapsed)
		}
		if e := errorx.From(err); e.Code != errorx.DeadlineExceeded || e.Reason != timeout.ReasonTimeout {
			t.Fatalf("err = %v, want the attempt timeout", err)
		}
		if be.count() != 3 {
			t.Fatalf("backend calls = %d, want 3", be.count())
		}

		// Fast failures are retried too; the breaker now holds 6 failures.
		be.set("fail")
		if _, err := ep(ctx, "req"); errorx.CodeOf(err) != errorx.Unavailable || errors.Is(err, circuitbreaker.ErrOpen) {
			t.Fatalf("err = %v, want the backend error", err)
		}
		if be.count() != 6 {
			t.Fatalf("backend calls = %d, want 6", be.count())
		}

		// The breaker rejects the first attempt and retry stops at once,
		// without backoff and without reaching the backend.
		start = time.Now()
		_, err = ep(ctx, "req")
		if !errors.Is(err, circuitbreaker.ErrOpen) {
			t.Fatalf("err = %v, want ErrOpen", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("elapsed = %v, want an immediate failure", elapsed)
		}
		if be.count() != 6 {
			t.Errorf("backend calls = %d, want 6: an open circuit must not be retried", be.count())
		}

		// Another operation has its own breaker and still reaches the backend.
		be.set("ok")
		other := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "/svc/List"})
		if resp, err := ep(other, "req2"); err != nil || resp != "req2" {
			t.Fatalf("other operation = %v, %v; want req2, nil", resp, err)
		}
	})
}

// TestClientChainRetryRecovers checks that a transient failure behind the
// breaker is retried and succeeds.
func TestClientChainRetryRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		be := &fakeBackend{mode: "hang"}
		ep := clientChain(be)
		go func() {
			// The backend recovers while the first attempt is hanging.
			time.Sleep(500 * time.Millisecond)
			be.set("ok")
		}()
		start := time.Now()
		resp, err := ep(t.Context(), "req")
		if err != nil || resp != "req" {
			t.Fatalf("ep = %v, %v; want req, nil", resp, err)
		}
		if elapsed := time.Since(start); elapsed != time.Second+100*time.Millisecond {
			t.Errorf("elapsed = %v, want 1.1s", elapsed)
		}
		if be.count() != 2 {
			t.Errorf("backend calls = %d, want 2", be.count())
		}
	})
}

// TestClientChainCallerDeadline checks that the caller's deadline bounds the
// whole chain, across attempts.
func TestClientChainCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		be := &fakeBackend{mode: "hang"}
		ep := clientChain(be)
		ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := ep(ctx, "req")
		if elapsed := time.Since(start); elapsed != 1500*time.Millisecond {
			t.Errorf("elapsed = %v, want the caller's 1.5s deadline", elapsed)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
		if be.count() != 2 {
			t.Errorf("backend calls = %d, want 2", be.count())
		}
	})
}
