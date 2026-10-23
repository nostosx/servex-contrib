package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// fakeClock is a manually advanced clock for WithClock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeRandom returns a settable value from WithRandom.
type fakeRandom struct{ bits atomic.Uint64 }

func newFakeRandom(v float64) *fakeRandom {
	r := &fakeRandom{}
	r.Set(v)
	return r
}

func (r *fakeRandom) Set(v float64)    { r.bits.Store(math.Float64bits(v)) }
func (r *fakeRandom) Float64() float64 { return math.Float64frombits(r.bits.Load()) }

// backend is a fake endpoint returning a settable error and counting calls.
type backend struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (b *backend) set(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

func (b *backend) endpoint(context.Context, any) (any, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.err != nil {
		return nil, b.err
	}
	return "ok", nil
}

func (b *backend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

var errDown = errorx.New(errorx.Unavailable, "DOWN", "backend down")

// call invokes ep n times and returns how many calls were rejected.
func call(t *testing.T, ep endpoint.Endpoint, ctx context.Context, n int) (rejected int) {
	t.Helper()
	for range n {
		if _, err := ep(ctx, nil); errors.Is(err, circuitbreaker.ErrOpen) {
			rejected++
		}
	}
	return rejected
}

func TestErrOpen(t *testing.T) {
	e := circuitbreaker.ErrOpen
	if e.Code != errorx.Unavailable || e.Reason != circuitbreaker.ReasonOpen || circuitbreaker.ReasonOpen != "CIRCUIT_OPEN" {
		t.Errorf("ErrOpen = %v, want Unavailable/CIRCUIT_OPEN", e)
	}
	if e.Retryable() || errorx.IsRetryable(e) {
		t.Error("ErrOpen must not be retryable")
	}
}

func TestIsFailure(t *testing.T) {
	failures := map[errorx.Code]bool{
		errorx.Unavailable: true, errorx.DeadlineExceeded: true, errorx.Internal: true,
		errorx.Unknown: true, errorx.ResourceExhausted: true,
	}
	for c := errorx.OK; c <= errorx.Unauthenticated; c++ {
		err := error(errorx.New(c, "", ""))
		if c == errorx.OK {
			err = nil
		}
		if got := circuitbreaker.IsFailure(err); got != failures[c] {
			t.Errorf("IsFailure(%v) = %v, want %v", c, got, failures[c])
		}
	}
	if !circuitbreaker.IsFailure(errors.New("plain")) {
		t.Error("a plain error (Unknown) must be a failure")
	}
	if !circuitbreaker.IsFailure(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must be a failure")
	}
	if circuitbreaker.IsFailure(context.Canceled) {
		t.Error("context.Canceled must not be a failure")
	}
}

func TestNoRejectionBelowMinRequests(t *testing.T) {
	be := &backend{err: errDown}
	ep := circuitbreaker.New(
		circuitbreaker.WithMinRequests(10),
		circuitbreaker.WithRandom(newFakeRandom(0).Float64),
	)(be.endpoint)
	if got := call(t, ep, t.Context(), 10); got != 0 {
		t.Fatalf("rejected %d of the first 10 calls, want 0", got)
	}
	// requests=10, accepts=0: p = 10/11 > 0, and the random draw 0 < p.
	if _, err := ep(t.Context(), nil); err != circuitbreaker.ErrOpen {
		t.Fatalf("11th call err = %v, want ErrOpen", err)
	}
	if be.count() != 10 {
		t.Errorf("backend calls = %d, want 10: rejected calls must not reach it", be.count())
	}
}

func TestDefaultMinRequests(t *testing.T) {
	be := &backend{err: errDown}
	ep := circuitbreaker.New(circuitbreaker.WithRandom(newFakeRandom(0).Float64))(be.endpoint)
	if got := call(t, ep, t.Context(), circuitbreaker.DefaultMinRequests); got != 0 {
		t.Fatalf("rejected %d calls below the default minimum, want 0", got)
	}
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("call above the default minimum was not rejected")
	}
}

// TestProbability checks the exact rejection threshold of the SRE formula.
func TestProbability(t *testing.T) {
	tests := []struct {
		name               string
		k                  float64
		failures, accepted int
	}{
		{"all failing", 2, 10, 0},
		{"half failing K=1.5", 1.5, 10, 10},
		{"mostly failing K=2", 2, 15, 5},
		{"mostly failing K=1", 1, 12, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := float64(tt.failures + tt.accepted)
			p := (requests - tt.k*float64(tt.accepted)) / (requests + 1)
			for _, draw := range []float64{p - 1e-9, p} {
				be := &backend{}
				r := newFakeRandom(0.999)
				ep := circuitbreaker.New(
					circuitbreaker.WithK(tt.k),
					circuitbreaker.WithMinRequests(0),
					circuitbreaker.WithRandom(r.Float64),
				)(be.endpoint)
				// Accepted calls first: with no failures yet p stays <= 0.
				call(t, ep, t.Context(), tt.accepted)
				be.set(errDown)
				call(t, ep, t.Context(), tt.failures)
				if be.count() != tt.failures+tt.accepted {
					t.Fatalf("calls were rejected while the draw was 0.999")
				}
				r.Set(draw)
				_, err := ep(t.Context(), nil)
				wantReject := draw < p
				if gotReject := err == circuitbreaker.ErrOpen; gotReject != wantReject {
					t.Errorf("p = %v, draw = %v: rejected = %v, want %v", p, draw, gotReject, wantReject)
				}
			}
		})
	}
}

// TestHealthyNeverRejects checks that p stays at 0 while accepts*K >= requests.
func TestHealthyNeverRejects(t *testing.T) {
	be := &backend{}
	r := newFakeRandom(0)
	ep := circuitbreaker.New(circuitbreaker.WithMinRequests(0), circuitbreaker.WithRandom(r.Float64))(be.endpoint)
	if got := call(t, ep, t.Context(), 100); got != 0 {
		t.Fatalf("healthy backend: %d rejections, want 0", got)
	}
	// 100 accepts tolerate failures until requests exceed 1.5*100: p is
	// evaluated before the call is counted, so 51 failures pass.
	be.set(errDown)
	if got := call(t, ep, t.Context(), 51); got != 0 {
		t.Fatalf("%d rejections within the K=1.5 headroom, want 0", got)
	}
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("call beyond the headroom was not rejected with a zero draw")
	}
}

func TestClientErrorsAreAccepts(t *testing.T) {
	for _, err := range []error{
		errorx.New(errorx.InvalidArgument, "BAD", "bad"),
		errorx.New(errorx.NotFound, "MISSING", "missing"),
		errorx.New(errorx.PermissionDenied, "", ""),
		errorx.New(errorx.Unauthenticated, "", ""),
		errorx.New(errorx.FailedPrecondition, "", ""),
	} {
		be := &backend{err: err}
		ep := circuitbreaker.New(circuitbreaker.WithMinRequests(0), circuitbreaker.WithRandom(newFakeRandom(0).Float64))(be.endpoint)
		if got := call(t, ep, t.Context(), 200); got != 0 {
			t.Errorf("%v: %d rejections, want 0", errorx.CodeOf(err), got)
		}
	}
}

func TestFailureCodesThrottle(t *testing.T) {
	for _, err := range []error{
		errorx.New(errorx.Unavailable, "", ""),
		errorx.New(errorx.DeadlineExceeded, "", ""),
		errorx.New(errorx.Internal, "", ""),
		errorx.New(errorx.ResourceExhausted, "", ""),
		errors.New("unknown"),
	} {
		be := &backend{err: err}
		ep := circuitbreaker.New(circuitbreaker.WithMinRequests(5), circuitbreaker.WithRandom(newFakeRandom(0).Float64))(be.endpoint)
		call(t, ep, t.Context(), 5)
		if got := call(t, ep, t.Context(), 1); got != 1 {
			t.Errorf("%v: call after 5 failures not rejected", errorx.CodeOf(err))
		}
	}
}

// TestRejectionsCountAsRequests checks that rejected calls raise requests
// but not accepts, so throttling persists while the backend is not tried.
func TestRejectionsCountAsRequests(t *testing.T) {
	be := &backend{}
	r := newFakeRandom(0.999)
	ep := circuitbreaker.New(circuitbreaker.WithK(1), circuitbreaker.WithMinRequests(0), circuitbreaker.WithRandom(r.Float64))(be.endpoint)
	call(t, ep, t.Context(), 10) // 10 accepts
	be.set(errDown)
	call(t, ep, t.Context(), 10) // 20 requests: p = 10/21
	// Force 79 rejections: requests 99, accepts 10, so p = 89/100.
	r.Set(0)
	if got := call(t, ep, t.Context(), 79); got != 79 {
		t.Fatalf("rejected %d, want 79", got)
	}
	r.Set(0.889)
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("draw 0.889 < p = 0.89 was not rejected: rejections are not counted as requests")
	}
	// requests 100, accepts 10, p = 90/101 ~ 0.891.
	r.Set(0.892)
	if got := call(t, ep, t.Context(), 1); got != 0 {
		t.Fatal("draw 0.892 > p was rejected")
	}
}

func TestWindowExpiry(t *testing.T) {
	clock := newFakeClock()
	be := &backend{err: errDown}
	ep := circuitbreaker.New(
		circuitbreaker.WithMinRequests(10),
		circuitbreaker.WithWindow(10*time.Second),
		circuitbreaker.WithBuckets(10),
		circuitbreaker.WithClock(clock.Now),
		circuitbreaker.WithRandom(newFakeRandom(0).Float64),
	)(be.endpoint)
	call(t, ep, t.Context(), 10)
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("not rejected after 10 failures")
	}
	// After a full window every bucket has expired.
	clock.Advance(10 * time.Second)
	if got := call(t, ep, t.Context(), 10); got != 0 {
		t.Fatalf("%d rejections after the window expired, want 0", got)
	}
}

// TestRollingBuckets checks that outcomes expire one bucket at a time.
func TestRollingBuckets(t *testing.T) {
	clock := newFakeClock()
	be := &backend{err: errDown}
	r := newFakeRandom(0)
	ep := circuitbreaker.New(
		circuitbreaker.WithMinRequests(10),
		circuitbreaker.WithWindow(4*time.Second),
		circuitbreaker.WithBuckets(4),
		circuitbreaker.WithClock(clock.Now),
		circuitbreaker.WithRandom(r.Float64),
	)(be.endpoint)
	call(t, ep, t.Context(), 6) // bucket 0: 6 failures
	clock.Advance(2 * time.Second)
	call(t, ep, t.Context(), 4) // bucket 2: 4 failures; total 10
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("not rejected with 10 failures in the window")
	}
	// Bucket 0 expires at 4s: 5 requests remain, below the minimum.
	clock.Advance(1999 * time.Millisecond)
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("bucket 0 expired too early")
	}
	clock.Advance(time.Millisecond)
	if got := call(t, ep, t.Context(), 1); got != 0 {
		t.Fatal("bucket 0 did not expire after a full window")
	}
}

func TestRecovery(t *testing.T) {
	clock := newFakeClock()
	be := &backend{err: errDown}
	// A seeded source: with a constant draw, a fully throttled breaker
	// would never let a probe through.
	r := rand.New(rand.NewPCG(1, 2))
	ep := circuitbreaker.New(
		circuitbreaker.WithMinRequests(10),
		circuitbreaker.WithClock(clock.Now),
		circuitbreaker.WithRandom(r.Float64),
	)(be.endpoint)
	if got := call(t, ep, t.Context(), 100); got == 0 {
		t.Fatal("failing backend was never throttled")
	}
	be.set(nil)
	// The backend recovered: every accepted probe lowers p, which lets more
	// probes through. Starting from full throttling this takes several
	// windows; probe at 10 calls per second for 150 seconds.
	for range 1500 {
		clock.Advance(100 * time.Millisecond)
		call(t, ep, t.Context(), 1)
	}
	if got := call(t, ep, t.Context(), 100); got != 0 {
		t.Fatalf("%d rejections after recovery, want 0", got)
	}
}

func TestKeyedBreakers(t *testing.T) {
	be := &backend{err: errDown}
	ep := circuitbreaker.New(circuitbreaker.WithMinRequests(5), circuitbreaker.WithRandom(newFakeRandom(0).Float64))(be.endpoint)
	ctxA := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "A"})
	ctxB := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "B"})
	call(t, ep, ctxA, 5)
	if got := call(t, ep, ctxA, 1); got != 1 {
		t.Fatal("operation A not throttled")
	}
	if got := call(t, ep, ctxB, 5); got != 0 {
		t.Fatalf("operation B: %d rejections caused by A's breaker, want 0", got)
	}
	// Calls without metadata share the "" breaker.
	if got := call(t, ep, t.Context(), 5); got != 0 {
		t.Fatalf("no metadata: %d rejections, want 0", got)
	}
}

func TestWithKey(t *testing.T) {
	type tenantKey struct{}
	be := &backend{err: errDown}
	ep := circuitbreaker.New(
		circuitbreaker.WithKey(func(ctx context.Context) string { s, _ := ctx.Value(tenantKey{}).(string); return s }),
		circuitbreaker.WithKey(nil), // keeps the previous key function
		circuitbreaker.WithMinRequests(3),
		circuitbreaker.WithRandom(newFakeRandom(0).Float64),
	)(be.endpoint)
	// Same operation, different tenants.
	base := metadata.NewContext(t.Context(), metadata.Info{Operation: "op"})
	ctx1 := context.WithValue(base, tenantKey{}, "t1")
	ctx2 := context.WithValue(base, tenantKey{}, "t2")
	call(t, ep, ctx1, 3)
	if call(t, ep, ctx1, 1) != 1 || call(t, ep, ctx2, 3) != 0 {
		t.Fatal("breakers are not separated by the custom key")
	}
}

func TestInvalidOptionsKeepDefaults(t *testing.T) {
	be := &backend{err: errDown}
	ep := circuitbreaker.New(
		nil,
		circuitbreaker.WithK(0), circuitbreaker.WithK(-1),
		circuitbreaker.WithWindow(0), circuitbreaker.WithBuckets(-2),
		circuitbreaker.WithMinRequests(-5), // treated as 0
		circuitbreaker.WithClock(nil), circuitbreaker.WithRandom(nil),
		circuitbreaker.WithRandom(newFakeRandom(0).Float64),
	)(be.endpoint)
	call(t, ep, t.Context(), 1)
	if got := call(t, ep, t.Context(), 1); got != 1 {
		t.Fatal("not rejected after a failure with no minimum")
	}
}

func TestPassesResponseAndError(t *testing.T) {
	want := errorx.New(errorx.NotFound, "X", "y")
	ep := circuitbreaker.New()(func(_ context.Context, req any) (any, error) {
		return fmt.Sprint(req, "!"), want
	})
	resp, err := ep(t.Context(), "hi")
	if resp != "hi!" || err != want {
		t.Fatalf("ep = %v, %v; want hi!, %v", resp, err, want)
	}
}

func TestConcurrent(t *testing.T) {
	be := &backend{}
	ep := circuitbreaker.New(circuitbreaker.WithMinRequests(0))(be.endpoint)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			ctx := metadata.NewContext(t.Context(), metadata.Info{Operation: fmt.Sprint("op", i%4)})
			for j := range 500 {
				if j%3 == 0 {
					be.set(errDown)
				} else {
					be.set(nil)
				}
				_, _ = ep(ctx, nil)
			}
		})
	}
	wg.Wait()
}

func BenchmarkAllow(b *testing.B) {
	ep := circuitbreaker.New()(func(context.Context, any) (any, error) { return nil, nil })
	ctx := metadata.NewContext(b.Context(), metadata.Info{Operation: "op"})
	for b.Loop() {
		_, _ = ep(ctx, nil)
	}
}
