package roundrobin

import (
	"context"
	"sync"

	"github.com/nostosx/servex/selector"
)

var _ selector.Selector = (*roundRobin)(nil)

// roundRobin is the smooth weighted round-robin selector.
type roundRobin struct {
	mu      sync.Mutex
	current map[string]*int // Addr -> current weight
	seen    map[string]bool // scratch set used to drop removed nodes
}

// New returns a smooth weighted round-robin selector. Nodes are identified by
// Addr and weighted by Node.Weight, where values <= 0 count as 1;
// the contrib shared client copies it from discovery.Instance.Weight. The
// selector is safe for concurrent use and its Done ignores call outcomes.
//
// On every Select each node's current weight grows by its weight, the node
// with the highest current weight is chosen (the first one on ties) and its
// current weight is lowered by the total weight. With weights 5, 1, 1 the
// picks repeat the sequence a a b a c a a. State is kept per Addr, so it
// survives changes of the node set; nodes that disappear are forgotten and
// start afresh if they return.
func New() selector.Selector {
	return &roundRobin{current: map[string]*int{}, seen: map[string]bool{}}
}

// noopDone is the Done of round robin, which ignores call outcomes.
func noopDone(context.Context, error) {}

// Select implements selector.Selector.
func (s *roundRobin) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	switch len(nodes) {
	case 0:
		return selector.Node{}, nil, selector.ErrNoAvailable
	case 1:
		return nodes[0], noopDone, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	total, best := 0, -1
	var bestCur *int
	for i, n := range nodes {
		w := max(n.Weight, 1)
		total += w
		cur, ok := s.current[n.Addr]
		if !ok {
			cur = new(0)
			s.current[n.Addr] = cur
		}
		*cur += w
		if best < 0 || *cur > *bestCur {
			best, bestCur = i, cur
		}
	}
	*bestCur -= total
	s.prune(nodes)
	return nodes[best], noopDone, nil
}

// prune forgets the state of nodes that are no longer offered. It only scans
// when the state holds more entries than nodes. s.mu must be held.
func (s *roundRobin) prune(nodes []selector.Node) {
	if len(s.current) <= len(nodes) {
		return
	}
	clear(s.seen)
	for _, n := range nodes {
		s.seen[n.Addr] = true
	}
	for addr := range s.current {
		if !s.seen[addr] {
			delete(s.current, addr)
		}
	}
	clear(s.seen)
}
