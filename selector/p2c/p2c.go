package p2c

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
)

// Defaults used by New.
const (
	// DefaultDecay is the default time constant of the latency EWMA.
	DefaultDecay = 2 * time.Second
	// DefaultPenalty is the default latency recorded for a failed call.
	DefaultPenalty = 500 * time.Millisecond
)

// unknownCost is the latency estimate, in nanoseconds, assumed for a node
// that has calls in flight but no estimate yet, such as a newly added node.
// It makes such a node the last choice until its first call completes, while
// still preferring the one with fewer calls in flight among such nodes.
const unknownCost = float64(math.MaxInt64)

var _ selector.Selector = (*p2c)(nil)

// Option configures the selector returned by New.
type Option func(*options)

type options struct {
	decay   time.Duration
	penalty time.Duration
	now     func() time.Time
	intN    func(n int) int
}

// WithDecay sets the time constant of the latency EWMA: the weight of past
// observations falls by a factor e every d. Shorter values react faster to
// latency changes and forget failures sooner. The default is DefaultDecay;
// non-positive values keep the default.
func WithDecay(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.decay = d
		}
	}
}

// WithPenalty sets the latency recorded for a failed call when the call
// itself was faster, so that a node failing fast does not look attractive.
// The default is DefaultPenalty; negative values keep the default.
func WithPenalty(d time.Duration) Option {
	return func(o *options) {
		if d >= 0 {
			o.penalty = d
		}
	}
}

// WithClock sets the clock used to measure latencies and decay the EWMA,
// mainly for tests. The default is time.Now; nil keeps the default. now must
// be safe for concurrent calls.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithRandom sets the source of uniformly distributed integers in [0, n)
// used to sample nodes, mainly for tests. It must be safe for concurrent
// use. The default is rand.IntN from math/rand/v2; nil keeps the default.
func WithRandom(intN func(n int) int) Option {
	return func(o *options) {
		if intN != nil {
			o.intN = intN
		}
	}
}

// New returns a Power of Two Choices selector.
//
// Select samples two distinct nodes and picks the one with the lower load,
// the first sampled one on ties. The load of a node is its latency estimate
// multiplied by its in-flight calls plus one and divided by its weight
// (Node.Weight, values <= 0 count as 1); a node with calls in flight and no
// estimate yet is avoided. The latency estimate is a peak-sensitive EWMA: a
// slower observation replaces it at once, faster ones and the passing of
// time pull it down, so a node that is no longer chosen becomes attractive
// again and is probed.
//
// The returned Done must be called once with the call's outcome; later calls
// are ignored. Errors whose errorx code is Unavailable, DeadlineExceeded,
// Internal, Unknown or ResourceExhausted are failures and record at least the
// penalty latency. A Canceled call only ends its in-flight count. Other
// errors, such as InvalidArgument, show that the node served the call and
// record its latency like a success.
//
// State is kept per Addr and survives changes of the node set; nodes that
// disappear are eventually forgotten. The selector is safe for concurrent
// use.
func New(opts ...Option) selector.Selector {
	o := options{
		decay:   DefaultDecay,
		penalty: DefaultPenalty,
		now:     time.Now,
		intN:    rand.IntN,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &p2c{opts: o, stats: map[string]*stats{}}
}

// p2c is the Power of Two Choices selector.
type p2c struct {
	opts options

	mu    sync.Mutex
	stats map[string]*stats // Addr -> state; pruned when it outgrows the nodes
	seen  map[string]bool   // scratch set used for pruning
}

// Select implements selector.Selector.
func (p *p2c) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	switch len(nodes) {
	case 0:
		return selector.Node{}, nil, selector.ErrNoAvailable
	case 1:
		return nodes[0], p.start(p.lookup(nodes, nodes[0].Addr)), nil
	}
	i := p.opts.intN(len(nodes))
	j := p.opts.intN(len(nodes) - 1)
	if j >= i {
		j++
	}
	a, b := p.lookup(nodes, nodes[i].Addr), p.lookup(nodes, nodes[j].Addr)
	now := p.opts.now()
	if b.load(now, nodes[j].Weight) < a.load(now, nodes[i].Weight) {
		i, a = j, b
	}
	return nodes[i], p.start(a), nil
}

// lookup returns the state of addr, creating it if needed, and prunes the
// state of nodes that are no longer offered.
func (p *p2c) lookup(nodes []selector.Node, addr string) *stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.stats[addr]
	if !ok {
		s = &stats{decay: float64(p.opts.decay), stamp: p.opts.now()}
		p.stats[addr] = s
		p.prune(nodes)
	}
	return s
}

// prune forgets the state of nodes not in nodes once the state holds twice
// as many entries as there are nodes, which keeps it amortized O(1). p.mu
// must be held.
func (p *p2c) prune(nodes []selector.Node) {
	if len(p.stats) <= 2*len(nodes) {
		return
	}
	if p.seen == nil {
		p.seen = map[string]bool{}
	}
	for _, n := range nodes {
		p.seen[n.Addr] = true
	}
	for addr := range p.stats {
		if !p.seen[addr] {
			delete(p.stats, addr)
		}
	}
	clear(p.seen)
}

// start records the beginning of a call to the node of s and returns its
// Done.
func (p *p2c) start(s *stats) selector.Done {
	s.inflight.Add(1)
	begin := p.opts.now()
	var once atomic.Bool
	return func(_ context.Context, err error) {
		if !once.CompareAndSwap(false, true) {
			return
		}
		defer s.inflight.Add(-1)
		now := p.opts.now()
		rtt := now.Sub(begin)
		switch code := errorx.CodeOf(err); {
		case code == errorx.Canceled:
			return
		case isFailure(code):
			rtt = max(rtt, p.opts.penalty)
		}
		s.observe(now, float64(max(rtt, 0)))
	}
}

// isFailure reports whether code shows that the node failed to serve a call.
func isFailure(code errorx.Code) bool {
	switch code {
	case errorx.Unavailable, errorx.DeadlineExceeded, errorx.Internal, errorx.Unknown, errorx.ResourceExhausted:
		return true
	default:
		return false
	}
}

// stats is the load state of one node.
type stats struct {
	decay    float64 // EWMA time constant in nanoseconds
	inflight atomic.Int64

	mu    sync.Mutex
	cost  float64   // peak EWMA of the latency in nanoseconds; 0 when unknown
	stamp time.Time // time of the last update of cost
}

// observe adds a latency sample of rtt nanoseconds taken at now. A sample
// above the estimate replaces it; otherwise the estimate moves towards the
// sample by a weight that grows with the time since the last update.
func (s *stats) observe(now time.Time, rtt float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.update(now, rtt)
}

// update implements observe. s.mu must be held.
func (s *stats) update(now time.Time, rtt float64) {
	elapsed := max(float64(now.Sub(s.stamp)), 0)
	s.stamp = now
	if rtt > s.cost {
		s.cost = rtt
		return
	}
	w := math.Exp(-elapsed / s.decay)
	s.cost = s.cost*w + rtt*(1-w)
}

// load returns the load of the node at now given its weight. Reading the
// load decays the latency estimate towards zero for the time elapsed since
// the last update, so idle nodes regain traffic.
func (s *stats) load(now time.Time, weight int) float64 {
	inflight := float64(max(s.inflight.Load(), 0))
	s.mu.Lock()
	s.update(now, 0)
	cost := s.cost
	s.mu.Unlock()
	if cost == 0 && inflight > 0 {
		cost = unknownCost
	}
	return cost * (inflight + 1) / float64(max(weight, 1))
}
