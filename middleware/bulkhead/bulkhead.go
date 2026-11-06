// Package bulkhead provides explicitly shared, fail-fast concurrency admission.
// It creates no goroutines or wait queues. Construct one limiter per isolation scope.
package bulkhead

import (
	"context"
	"errors"
	"sync"

	"github.com/nostosx/servex/errorx"
)

type Limiter struct{ slots chan struct{} }

// New creates one shared admission scope with a positive concurrent-call limit.
func New(capacity int) (*Limiter, error) {
	if capacity <= 0 {
		return nil, errors.New("bulkhead: capacity must be positive")
	}
	return &Limiter{slots: make(chan struct{}, capacity)}, nil
}

// Acquire reserves one slot or rejects immediately. The returned release function
// is idempotent: later calls are ignored, so a double release cannot withdraw a
// slot held by another caller and silently exceed the configured capacity.
func (l *Limiter) Acquire(ctx context.Context) (func(), error) {
	if ctx.Err() != nil {
		return nil, errors.Join(ctx.Err(), context.Cause(ctx))
	}
	select {
	case l.slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-l.slots }) }, nil
	default:
		return nil, errorx.New(errorx.ResourceExhausted, "BULKHEAD_FULL", "concurrency limit reached")
	}
}
