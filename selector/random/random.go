package random

import (
	"context"
	"math/rand/v2"

	"github.com/nostosx/servex/selector"
)

// New returns a stateless selector.Selector that picks nodes at random with
// probability proportional to their Weight. It uses math/rand/v2.
func New() selector.Selector {
	return &random{intN: rand.IntN}
}

// random is the weighted random selector.Selector. intN is injectable for tests.
type random struct {
	intN func(n int) int
}

var _ selector.Selector = (*random)(nil)

// noopDone is the selector.Done returned by stateless selectors.
func noopDone(context.Context, error) {}

// Select implements selector.Selector.
func (r *random) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	switch len(nodes) {
	case 0:
		return selector.Node{}, nil, selector.ErrNoAvailable
	case 1:
		return nodes[0], noopDone, nil
	}
	total := 0
	for _, n := range nodes {
		total += weight(n)
	}
	pick := r.intN(total)
	for _, n := range nodes {
		pick -= weight(n)
		if pick < 0 {
			return n, noopDone, nil
		}
	}
	// Unreachable for a well-behaved intN; fall back to the last node.
	return nodes[len(nodes)-1], noopDone, nil
}

func weight(n selector.Node) int { return max(n.Weight, 1) }
