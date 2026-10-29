package random

import (
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
)

func TestErrNoAvailable(t *testing.T) {
	e, ok := errors.AsType[*errorx.Error](error(selector.ErrNoAvailable))
	if !ok {
		t.Fatal("selector.ErrNoAvailable is not an *errorx.Error")
	}
	if e.Code != errorx.Unavailable {
		t.Errorf("Code = %v, want Unavailable", e.Code)
	}
	if e.Reason != "NO_AVAILABLE_NODE" {
		t.Errorf("Reason = %q, want NO_AVAILABLE_NODE", e.Reason)
	}
	if !e.Retryable() {
		t.Error("selector.ErrNoAvailable should be retryable")
	}
}

func TestRandomEmpty(t *testing.T) {
	for _, nodes := range [][]selector.Node{nil, {}} {
		_, done, err := New().Select(t.Context(), nodes)
		if !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("Select(%v) error = %v, want selector.ErrNoAvailable", nodes, err)
		}
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("CodeOf = %v, want Unavailable", errorx.CodeOf(err))
		}
		if done != nil {
			t.Fatal("selector.Done must be nil on error")
		}
	}
}

func TestRandomSingle(t *testing.T) {
	nodes := []selector.Node{{Addr: "a:1", Weight: -5}}
	for range 10 {
		n, done, err := New().Select(t.Context(), nodes)
		if err != nil {
			t.Fatalf("Select error = %v", err)
		}
		if n.Addr != "a:1" {
			t.Fatalf("Addr = %q, want a:1", n.Addr)
		}
		if done == nil {
			t.Fatal("selector.Done is nil")
		}
		done(t.Context(), nil)
	}
}

// TestRandomWeightMapping checks the exact mapping from random draws to
// nodes with a deterministic source.
func TestRandomWeightMapping(t *testing.T) {
	nodes := []selector.Node{
		{Addr: "a", Weight: 3},
		{Addr: "b", Weight: 0},  // counts as 1
		{Addr: "c", Weight: -2}, // counts as 1
		{Addr: "d", Weight: 5},
	}
	want := []string{"a", "a", "a", "b", "c", "d", "d", "d", "d", "d"}
	for draw, addr := range want {
		r := &random{intN: func(n int) int {
			if n != 10 {
				t.Fatalf("intN called with %d, want total weight 10", n)
			}
			return draw
		}}
		n, done, err := r.Select(t.Context(), nodes)
		if err != nil {
			t.Fatalf("Select error = %v", err)
		}
		if done == nil {
			t.Fatal("selector.Done is nil")
		}
		if n.Addr != addr {
			t.Errorf("draw %d: Addr = %q, want %q", draw, n.Addr, addr)
		}
	}
}

func TestRandomDistribution(t *testing.T) {
	nodes := []selector.Node{
		{Addr: "a", Weight: 1},
		{Addr: "b", Weight: 2},
		{Addr: "c", Weight: 7},
		{Addr: "d"}, // counts as 1
	}
	const iterations = 110_000
	tests := []struct {
		name string
		sel  selector.Selector
	}{
		{"seeded", &random{intN: rand.New(rand.NewPCG(1, 2)).IntN}},
		{"global", New()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts := map[string]int{}
			for range iterations {
				n, _, err := tt.sel.Select(t.Context(), nodes)
				if err != nil {
					t.Fatalf("Select error = %v", err)
				}
				counts[n.Addr]++
			}
			total := 0
			for _, n := range nodes {
				total += weight(n)
			}
			for _, n := range nodes {
				want := float64(iterations) * float64(weight(n)) / float64(total)
				got := float64(counts[n.Addr])
				// A 10% relative tolerance is far beyond the expected noise
				// (well under 2% at this sample size).
				if math.Abs(got-want) > want*0.10 {
					t.Errorf("node %s: got %v picks, want about %v", n.Addr, got, want)
				}
			}
		})
	}
}

func TestRandomConcurrent(t *testing.T) {
	sel := New()
	nodes := []selector.Node{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 2}}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				n, done, err := sel.Select(t.Context(), nodes)
				if err != nil || done == nil || (n.Addr != "a" && n.Addr != "b") {
					t.Errorf("Select = %v, done nil %v, err %v", n, done == nil, err)
					return
				}
				done(t.Context(), nil)
			}
		})
	}
	wg.Wait()
}

func BenchmarkRandom(b *testing.B) {
	sel := New()
	nodes := []selector.Node{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 2}, {Addr: "c", Weight: 3}}
	ctx := b.Context()
	for b.Loop() {
		_, done, _ := sel.Select(ctx, nodes)
		done(ctx, nil)
	}
}
