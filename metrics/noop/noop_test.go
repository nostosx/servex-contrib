package noop_test

import (
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/metrics/noop"
	"github.com/nostosx/servex/observability/metrics"
)

func TestNoopUsable(t *testing.T) {
	m := noop.New()
	if m == nil {
		t.Fatal("Noop returned nil")
	}
	c := m.Counter("requests_total", "help", "method")
	g := m.Gauge("in_flight", "help")
	h := m.Histogram("latency_seconds", "help", metrics.DefaultBuckets, "method", "code")
	if c == nil || g == nil || h == nil {
		t.Fatal("Noop meter returned a nil metric")
	}
	if c.With("GET") == nil || g.With() == nil || h.With("GET", "200") == nil {
		t.Fatal("With returned nil")
	}
	// Mismatched label counts must not panic on the no-op implementation.
	ctx := t.Context()
	c.With().Add(ctx, 1)
	c.With("a", "b", "c").Add(ctx, 1)
	g.Set(ctx, 3)
	g.Add(ctx, -1)
	g.With("x").Set(ctx, 1)
	h.Observe(ctx, 0.2)
	h.With().Observe(ctx, 1)
}

func TestNoopDoesNotAllocate(t *testing.T) {
	m := noop.New()
	c := m.Counter("c", "h", "l")
	g := m.Gauge("g", "h", "l")
	h := m.Histogram("h", "h", nil, "l")
	ctx := t.Context()
	allocs := testing.AllocsPerRun(100, func() {
		_ = noop.New()
		c.With("v").Add(ctx, 1)
		g.With("v").Set(ctx, 1)
		g.With("v").Add(ctx, 1)
		h.With("v").Observe(ctx, 1)
	})
	if allocs != 0 {
		t.Fatalf("noop metrics allocated %v times per run, want 0", allocs)
	}
}

func TestNoopConcurrentUse(t *testing.T) {
	m := noop.New()
	ctx := t.Context()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			m.Counter("c", "h", "l").With("v").Add(ctx, float64(i))
			m.Gauge("g", "h").Set(ctx, float64(i))
			m.Histogram("h", "h", metrics.DefaultBuckets).Observe(ctx, float64(i))
		})
	}
	wg.Wait()
}

func BenchmarkNoopCounter(b *testing.B) {
	c := noop.New().Counter("c", "h", "l")
	ctx := b.Context()
	for b.Loop() {
		c.With("v").Add(ctx, 1)
	}
}
