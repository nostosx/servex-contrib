package roundrobin

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex/selector"
)

// picks returns the addresses of n consecutive selections.
func picks(t *testing.T, s selector.Selector, nodes []selector.Node, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for range n {
		node, done, err := s.Select(t.Context(), nodes)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if done == nil {
			t.Fatal("Select returned a nil Done")
		}
		done(t.Context(), nil)
		out = append(out, node.Addr)
	}
	return out
}

func TestEmpty(t *testing.T) {
	for _, nodes := range [][]selector.Node{nil, {}} {
		_, done, err := New().Select(t.Context(), nodes)
		if !errors.Is(err, selector.ErrNoAvailable) || done != nil {
			t.Fatalf("Select(%v) = done nil %v, %v; want nil, ErrNoAvailable", nodes, done == nil, err)
		}
	}
}

func TestSingle(t *testing.T) {
	got := picks(t, New(), []selector.Node{{Addr: "a", Weight: 7}}, 3)
	if strings.Join(got, "") != "aaa" {
		t.Fatalf("picks = %v, want a a a", got)
	}
}

// TestSmoothSequence checks the nginx sequence for weights 5, 1, 1.
func TestSmoothSequence(t *testing.T) {
	nodes := []selector.Node{{Addr: "a", Weight: 5}, {Addr: "b", Weight: 1}, {Addr: "c", Weight: 1}}
	got := strings.Join(picks(t, New(), nodes, 21), "")
	if want := strings.Repeat("aabacaa", 3); got != want {
		t.Fatalf("picks = %s, want %s", got, want)
	}
}

func TestEqualWeightsRotate(t *testing.T) {
	nodes := []selector.Node{{Addr: "a"}, {Addr: "b", Weight: -1}, {Addr: "c", Weight: 1}}
	got := strings.Join(picks(t, New(), nodes, 9), "")
	if got != "abcabcabc" {
		t.Fatalf("picks = %s, want abcabcabc", got)
	}
}

// TestExactDistribution checks that every cycle of total-weight picks
// follows the weights exactly.
func TestExactDistribution(t *testing.T) {
	nodes := []selector.Node{
		{Addr: "a", Weight: 3}, {Addr: "b", Weight: 2}, {Addr: "c", Weight: 1},
		{Addr: "d", Weight: 0}, {Addr: "e", Weight: -4}, {Addr: "f", Weight: 10},
	}
	const total = 3 + 2 + 1 + 1 + 1 + 10
	s := New()
	for cycle := range 50 {
		counts := map[string]int{}
		for _, addr := range picks(t, s, nodes, total) {
			counts[addr]++
		}
		for _, n := range nodes {
			if want := max(n.Weight, 1); counts[n.Addr] != want {
				t.Fatalf("cycle %d: node %s picked %d times, want %d", cycle, n.Addr, counts[n.Addr], want)
			}
		}
	}
}

// TestSmoothness checks that a heavy node is interleaved with the others
// rather than picked in one burst.
func TestSmoothness(t *testing.T) {
	nodes := []selector.Node{{Addr: "a", Weight: 2}, {Addr: "b", Weight: 2}, {Addr: "c", Weight: 2}, {Addr: "d", Weight: 2}}
	got := strings.Join(picks(t, New(), nodes, 8), "")
	if got != "abcdabcd" {
		t.Fatalf("picks = %s, want abcdabcd", got)
	}
}

func TestNodeSetChanges(t *testing.T) {
	s := New().(*roundRobin)
	abc := []selector.Node{{Addr: "a"}, {Addr: "b"}, {Addr: "c"}}
	if got := strings.Join(picks(t, s, abc, 2), ""); got != "ab" {
		t.Fatalf("picks = %s, want ab", got)
	}
	// c is removed and d added: the state of c is dropped.
	abd := []selector.Node{{Addr: "a"}, {Addr: "b"}, {Addr: "d"}}
	counts := map[string]int{}
	for _, addr := range picks(t, s, abd, 30) {
		counts[addr]++
	}
	for _, addr := range []string{"a", "b", "d"} {
		if counts[addr] != 10 {
			t.Errorf("node %s picked %d of 30 times, want 10", addr, counts[addr])
		}
	}
	s.mu.Lock()
	_, kept := s.current["c"]
	size := len(s.current)
	s.mu.Unlock()
	if kept || size != 3 {
		t.Errorf("state = %d entries (c kept: %v), want 3 without c", size, kept)
	}
}

func TestConcurrent(t *testing.T) {
	s := New()
	nodes := []selector.Node{{Addr: "a", Weight: 1}, {Addr: "b", Weight: 2}, {Addr: "c", Weight: 5}}
	const goroutines, perGoroutine = 16, 1000
	var mu sync.Mutex
	counts := map[string]int{}
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			local := map[string]int{}
			for range perGoroutine {
				n, done, err := s.Select(t.Context(), nodes)
				if err != nil {
					t.Errorf("Select: %v", err)
					return
				}
				done(t.Context(), nil)
				local[n.Addr]++
			}
			mu.Lock()
			for k, v := range local {
				counts[k] += v
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	// Selections are serialized, so 16000 picks are 2000 full cycles of 8.
	for _, n := range nodes {
		if want := goroutines * perGoroutine / 8 * n.Weight; counts[n.Addr] != want {
			t.Errorf("node %s picked %d times, want %d", n.Addr, counts[n.Addr], want)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	s := New()
	nodes := make([]selector.Node, 10)
	for i := range nodes {
		nodes[i] = selector.Node{Addr: fmt.Sprint("10.0.0.", i, ":80"), Weight: i}
	}
	ctx := b.Context()
	for b.Loop() {
		_, done, _ := s.Select(ctx, nodes)
		done(ctx, nil)
	}
}
