package p2c

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
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

// pairs is a WithRandom source that makes Select sample the given index
// pairs in order, cycling. For a pair (i, j) with i != j it returns i for
// the first draw and the value that maps to j for the second one.
type pairs struct {
	mu    sync.Mutex
	seq   [][2]int
	pos   int
	draws int
}

func (p *pairs) intN(int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pair := p.seq[p.pos%len(p.seq)]
	p.draws++
	if p.draws%2 == 1 {
		return pair[0]
	}
	p.pos++
	if pair[1] > pair[0] {
		return pair[1] - 1
	}
	return pair[1]
}

var (
	errDown    = errorx.New(errorx.Unavailable, "DOWN", "backend down")
	errInvalid = errorx.New(errorx.InvalidArgument, "BAD", "bad request")
)

// newTest returns a selector with a fake clock that samples (0, 1) and then
// (1, 0), so both orders are exercised.
func newTest(opts ...Option) (*p2c, *fakeClock) {
	clock := newFakeClock()
	src := &pairs{seq: [][2]int{{0, 1}, {1, 0}}}
	s := New(append([]Option{WithClock(clock.Now), WithRandom(src.intN)}, opts...)...).(*p2c)
	return s, clock
}

// call selects a node, advances the clock by rtt and reports err.
func call(t *testing.T, s selector.Selector, clock *fakeClock, nodes []selector.Node, rtt time.Duration, err error) string {
	t.Helper()
	n, done, selErr := s.Select(t.Context(), nodes)
	if selErr != nil {
		t.Fatalf("Select: %v", selErr)
	}
	clock.Advance(rtt)
	done(t.Context(), err)
	return n.Addr
}

// prime records one call of the given latency and outcome on the node addr.
func prime(t *testing.T, s *p2c, clock *fakeClock, addr string, rtt time.Duration, err error) {
	t.Helper()
	call(t, s, clock, []selector.Node{{Addr: addr}}, rtt, err)
}

func twoNodes() []selector.Node { return []selector.Node{{Addr: "a"}, {Addr: "b"}} }

func TestEmpty(t *testing.T) {
	for _, nodes := range [][]selector.Node{nil, {}} {
		_, done, err := New().Select(t.Context(), nodes)
		if !errors.Is(err, selector.ErrNoAvailable) || done != nil {
			t.Fatalf("Select(%v) = done nil %v, %v; want nil, ErrNoAvailable", nodes, done == nil, err)
		}
	}
}

func TestSingle(t *testing.T) {
	s := New(WithRandom(func(int) int { panic("random source used for a single node") }))
	for range 3 {
		n, done, err := s.Select(t.Context(), []selector.Node{{Addr: "a"}})
		if err != nil || n.Addr != "a" || done == nil {
			t.Fatalf("Select = %v, done nil %v, %v", n, done == nil, err)
		}
		done(t.Context(), nil)
	}
}

// TestSamplesDistinctNodes checks that the two candidates always differ.
func TestSamplesDistinctNodes(t *testing.T) {
	nodes := []selector.Node{{Addr: "a"}, {Addr: "b"}, {Addr: "c"}, {Addr: "d"}}
	for i := range nodes {
		for j := range len(nodes) - 1 {
			draws := []int{i, j}
			var seen []int
			s := New(WithRandom(func(n int) int {
				seen = append(seen, n)
				d := draws[0]
				draws = draws[1:]
				return d
			})).(*p2c)
			if _, _, err := s.Select(t.Context(), nodes); err != nil {
				t.Fatal(err)
			}
			if len(seen) != 2 || seen[0] != 4 || seen[1] != 3 {
				t.Fatalf("random bounds = %v, want [4 3]", seen)
			}
			s.mu.Lock()
			sampled := len(s.stats)
			s.mu.Unlock()
			if sampled != 2 {
				t.Fatalf("draws (%d, %d) sampled %d distinct nodes, want 2", i, j, sampled)
			}
		}
	}
}

func TestPrefersLowerLatency(t *testing.T) {
	s, clock := newTest()
	prime(t, s, clock, "a", 10*time.Millisecond, nil)
	prime(t, s, clock, "b", 100*time.Millisecond, nil)
	for i := range 6 {
		if got := call(t, s, clock, twoNodes(), 10*time.Millisecond, nil); got != "a" {
			t.Fatalf("call %d picked %s, want the faster a", i, got)
		}
	}
}

func TestPrefersFewerInflight(t *testing.T) {
	s, clock := newTest()
	prime(t, s, clock, "a", 10*time.Millisecond, nil)
	prime(t, s, clock, "b", 10*time.Millisecond, nil)
	// Hold three calls on a, so its load is (3+1) times its latency.
	for range 3 {
		_, done, _ := s.Select(t.Context(), []selector.Node{{Addr: "a"}})
		defer done(t.Context(), nil)
	}
	for i := range 4 {
		n, done, _ := s.Select(t.Context(), twoNodes())
		if n.Addr != "b" {
			t.Fatalf("call %d picked %s, want b with fewer calls in flight", i, n.Addr)
		}
		done(t.Context(), nil)
	}
}

func TestWeight(t *testing.T) {
	s, clock := newTest()
	prime(t, s, clock, "a", 30*time.Millisecond, nil)
	prime(t, s, clock, "b", 20*time.Millisecond, nil)
	// a is slower but has twice the capacity of b: 30/2 < 20.
	nodes := []selector.Node{{Addr: "a", Weight: 2}, {Addr: "b", Weight: -1}}
	for i := range 4 {
		if got := call(t, s, clock, nodes, 0, nil); got != "a" {
			t.Fatalf("call %d picked %s, want a with the lower weighted load", i, got)
		}
	}
}

func TestErrorPenalty(t *testing.T) {
	s, clock := newTest(WithPenalty(time.Second))
	prime(t, s, clock, "a", 10*time.Millisecond, nil)
	prime(t, s, clock, "b", 50*time.Millisecond, nil)
	// a fails fast: its 1ms failure is recorded as the 1s penalty.
	prime(t, s, clock, "a", time.Millisecond, errDown)
	if got := s.stats["a"].cost; got != float64(time.Second) {
		t.Fatalf("cost after failure = %v, want the 1s penalty", time.Duration(got))
	}
	if got := call(t, s, clock, twoNodes(), 50*time.Millisecond, nil); got != "b" {
		t.Fatalf("picked %s, want b after a failed", got)
	}
}

func TestFailureCodes(t *testing.T) {
	for _, tt := range []struct {
		err     error
		penalty bool
	}{
		{errDown, true},
		{errorx.New(errorx.DeadlineExceeded, "", ""), true},
		{errorx.New(errorx.Internal, "", ""), true},
		{errorx.New(errorx.ResourceExhausted, "", ""), true},
		{errors.New("plain"), true},
		{errInvalid, false},
		{errorx.New(errorx.NotFound, "", ""), false},
		{nil, false},
	} {
		s, clock := newTest(WithPenalty(time.Second))
		prime(t, s, clock, "a", 5*time.Millisecond, tt.err)
		want := 5 * time.Millisecond
		if tt.penalty {
			want = time.Second
		}
		if got := time.Duration(s.stats["a"].cost); got != want {
			t.Errorf("err %v: cost = %v, want %v", tt.err, got, want)
		}
	}
}

// TestCanceledNotObserved checks that a canceled call only ends its
// in-flight count.
func TestCanceledNotObserved(t *testing.T) {
	s, clock := newTest()
	prime(t, s, clock, "a", 10*time.Millisecond, nil)
	prime(t, s, clock, "a", time.Hour, context.Canceled)
	st := s.stats["a"]
	if got := time.Duration(st.cost); got != 10*time.Millisecond {
		t.Errorf("cost = %v, want 10ms unchanged", got)
	}
	if got := st.inflight.Load(); got != 0 {
		t.Errorf("inflight = %d, want 0", got)
	}
}

func TestDoneIdempotent(t *testing.T) {
	s, clock := newTest()
	_, done, _ := s.Select(t.Context(), []selector.Node{{Addr: "a"}})
	if got := s.stats["a"].inflight.Load(); got != 1 {
		t.Fatalf("inflight = %d, want 1", got)
	}
	clock.Advance(time.Millisecond)
	done(t.Context(), nil)
	clock.Advance(time.Hour)
	done(t.Context(), errDown)
	st := s.stats["a"]
	if got := st.inflight.Load(); got != 0 {
		t.Errorf("inflight = %d, want 0", got)
	}
	if got := time.Duration(st.cost); got != time.Millisecond {
		t.Errorf("cost = %v, want 1ms: later Done calls must be ignored", got)
	}
}

// TestPeakEWMA checks the estimator: slower samples replace it, faster ones
// move it by the time-dependent weight, and idle time decays it.
func TestPeakEWMA(t *testing.T) {
	clock := newFakeClock()
	st := &stats{decay: float64(time.Second), stamp: clock.Now()}
	st.observe(clock.Now(), 100)
	if st.cost != 100 {
		t.Fatalf("cost = %v, want 100 after the first sample", st.cost)
	}
	st.observe(clock.Now(), 300)
	if st.cost != 300 {
		t.Fatalf("cost = %v, want the peak 300", st.cost)
	}
	// A faster sample one time constant later: weight e^-1 on the past.
	clock.Advance(time.Second)
	st.observe(clock.Now(), 100)
	if want := 300/2.718281828459045 + 100*(1-1/2.718281828459045); diff(st.cost, want) > 1e-9 {
		t.Fatalf("cost = %v, want %v", st.cost, want)
	}
	// A faster sample at the same instant has no weight.
	before := st.cost
	st.observe(clock.Now(), 0)
	if st.cost != before {
		t.Fatalf("cost = %v, want %v unchanged", st.cost, before)
	}
	// Reading the load decays towards zero.
	clock.Advance(10 * time.Second)
	if load := st.load(clock.Now(), 1); load > before*0.0001 {
		t.Fatalf("load after 10 time constants = %v, want close to 0", load)
	}
}

func diff(a, b float64) float64 { return max(a-b, b-a) }

// TestFailedNodeRecovers checks that a penalized node is probed again once
// its estimate has decayed, and regains traffic when it is healthy.
func TestFailedNodeRecovers(t *testing.T) {
	s, clock := newTest(WithDecay(time.Second), WithPenalty(time.Second))
	prime(t, s, clock, "a", 10*time.Millisecond, errDown)
	prime(t, s, clock, "b", 10*time.Millisecond, nil)
	counts := map[string]int{}
	// One call every 100ms for 20s; both nodes are healthy now.
	for range 200 {
		addr := call(t, s, clock, twoNodes(), 10*time.Millisecond, nil)
		counts[addr]++
		clock.Advance(90 * time.Millisecond)
	}
	if counts["a"] < 50 {
		t.Fatalf("recovered node got %d of 200 calls, want a fair share: %v", counts["a"], counts)
	}
}

// TestNewNodeNotFlooded checks that a node without an estimate receives one
// call and is then avoided until that call completes.
func TestNewNodeNotFlooded(t *testing.T) {
	s, clock := newTest()
	prime(t, s, clock, "a", 50*time.Millisecond, nil)
	nodes := twoNodes()
	first, done, _ := s.Select(t.Context(), nodes)
	if first.Addr != "b" {
		t.Fatalf("first call picked %s, want the unknown node b", first.Addr)
	}
	for i := range 4 {
		n, d, _ := s.Select(t.Context(), nodes)
		if n.Addr != "a" {
			t.Fatalf("call %d picked %s, want a while b's first call is in flight", i, n.Addr)
		}
		defer d(t.Context(), nil)
	}
	clock.Advance(5 * time.Millisecond)
	done(t.Context(), nil)
	if got := call(t, s, clock, nodes, 5*time.Millisecond, nil); got != "b" {
		t.Fatalf("picked %s, want b once its fast first call completed", got)
	}
}

// simulate runs n sequential calls with latencies per node and returns the
// number of calls each node received. Nodes listed in failing fail.
func simulate(t *testing.T, latency map[string]time.Duration, failing map[string]bool, n int) map[string]int {
	t.Helper()
	clock := newFakeClock()
	r := rand.New(rand.NewPCG(7, 11))
	s := New(WithClock(clock.Now), WithRandom(r.IntN))
	nodes := make([]selector.Node, 0, len(latency))
	for _, addr := range []string{"a", "b", "c", "d"} {
		if _, ok := latency[addr]; ok {
			nodes = append(nodes, selector.Node{Addr: addr})
		}
	}
	counts := map[string]int{}
	for range n {
		node, done, err := s.Select(t.Context(), nodes)
		if err != nil {
			t.Fatal(err)
		}
		counts[node.Addr]++
		clock.Advance(latency[node.Addr])
		var callErr error
		if failing[node.Addr] {
			callErr = errDown
		}
		done(t.Context(), callErr)
		clock.Advance(time.Millisecond)
	}
	return counts
}

func TestDistributionFollowsLatency(t *testing.T) {
	latency := map[string]time.Duration{"a": 5 * time.Millisecond, "b": 20 * time.Millisecond, "c": 200 * time.Millisecond}
	counts := simulate(t, latency, nil, 10_000)
	if counts["a"] <= counts["b"] || counts["b"] <= counts["c"] {
		t.Fatalf("counts = %v, want a > b > c", counts)
	}
	if counts["c"] > 1000 {
		t.Errorf("slowest node got %d of 10000 calls, want under 10%%", counts["c"])
	}
	if counts["a"] < 5000 {
		t.Errorf("fastest node got %d of 10000 calls, want over half", counts["a"])
	}
}

func TestDistributionAvoidsFailingNode(t *testing.T) {
	latency := map[string]time.Duration{"a": 10 * time.Millisecond, "b": 10 * time.Millisecond, "c": time.Millisecond}
	counts := simulate(t, latency, map[string]bool{"c": true}, 10_000)
	if counts["c"] > 500 {
		t.Fatalf("failing node got %d of 10000 calls, want under 5%%: %v", counts["c"], counts)
	}
	// The healthy nodes share the rest evenly.
	if d := counts["a"] - counts["b"]; d > 1000 || d < -1000 {
		t.Errorf("healthy nodes unbalanced: %v", counts)
	}
}

func TestDistributionEqualNodes(t *testing.T) {
	latency := map[string]time.Duration{"a": 10 * time.Millisecond, "b": 10 * time.Millisecond, "c": 10 * time.Millisecond, "d": 10 * time.Millisecond}
	counts := simulate(t, latency, nil, 20_000)
	for addr, c := range counts {
		if c < 4000 || c > 6000 {
			t.Errorf("node %s got %d of 20000 calls, want about 5000: %v", addr, c, counts)
		}
	}
}

func TestPrune(t *testing.T) {
	s, clock := newTest()
	for i := range 10 {
		call(t, s, clock, []selector.Node{{Addr: fmt.Sprint("old", i)}}, time.Millisecond, nil)
	}
	nodes := twoNodes()
	call(t, s, clock, nodes, time.Millisecond, nil)
	call(t, s, clock, nodes, time.Millisecond, nil)
	// Removed nodes are forgotten lazily: the state never holds more than
	// twice the offered nodes, and the offered ones are always kept.
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stats) > 2*len(nodes) {
		t.Fatalf("state holds %d entries for %d nodes", len(s.stats), len(nodes))
	}
	for _, n := range nodes {
		if s.stats[n.Addr] == nil {
			t.Errorf("state of offered node %s missing", n.Addr)
		}
	}
	for i := range 8 {
		if s.stats[fmt.Sprint("old", i)] != nil {
			t.Errorf("state of long-removed node old%d kept", i)
		}
	}
}

func TestInvalidOptionsKeepDefaults(t *testing.T) {
	s := New(nil, WithDecay(0), WithDecay(-time.Second), WithPenalty(-1), WithClock(nil), WithRandom(nil)).(*p2c)
	if s.opts.decay != DefaultDecay || s.opts.penalty != DefaultPenalty || s.opts.now == nil || s.opts.intN == nil {
		t.Fatalf("options = %+v, want defaults", s.opts)
	}
	if s := New(WithPenalty(0)).(*p2c); s.opts.penalty != 0 {
		t.Errorf("WithPenalty(0) = %v, want 0", s.opts.penalty)
	}
}

func TestConcurrent(t *testing.T) {
	s := New()
	nodes := []selector.Node{{Addr: "a"}, {Addr: "b"}, {Addr: "c"}}
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 500 {
				n, done, err := s.Select(t.Context(), nodes)
				if err != nil {
					t.Errorf("Select: %v", err)
					return
				}
				var callErr error
				if (g+i)%7 == 0 {
					callErr = errDown
				}
				_ = n
				done(t.Context(), callErr)
			}
		})
	}
	wg.Wait()
	p := s.(*p2c)
	for addr, st := range p.stats {
		if got := st.inflight.Load(); got != 0 {
			t.Errorf("node %s: inflight = %d after all calls completed", addr, got)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	s := New()
	nodes := make([]selector.Node, 10)
	for i := range nodes {
		nodes[i] = selector.Node{Addr: fmt.Sprint("10.0.0.", i, ":80")}
	}
	ctx := b.Context()
	for b.Loop() {
		_, done, _ := s.Select(ctx, nodes)
		done(ctx, nil)
	}
}
