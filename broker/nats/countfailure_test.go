package nats

import (
	"context"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// ctxFailures is a failure store that, like a KeyValue bucket, fails once
// the context of a call has ended.
type ctxFailures struct {
	mu sync.Mutex
	n  map[uint64]int
}

func (f *ctxFailures) get(ctx context.Context, seq uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n[seq], nil
}

func (f *ctxFailures) set(ctx context.Context, seq uint64, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n[seq] = n
	return nil
}

func (f *ctxFailures) clear(ctx context.Context, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.n, seq)
	return nil
}

func TestCountFailureWhileStopping(t *testing.T) {
	store := &ctxFailures{n: map[uint64]int{}}
	s := &subscription{b: New(nil), failures: store}
	// Earlier deliveries were interrupted (for example by restarts), which
	// does not count as failures.
	md := &jetstream.MsgMetadata{NumDelivered: 5, Sequence: jetstream.SequencePair{Stream: 7}}

	// The subscription stops while the failure is being recorded.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n := s.countFailure(ctx, md); n != 1 {
		t.Fatalf("countFailure() = %d, want 1: a stopping subscription must still use the failure store", n)
	}
	if n, _ := store.get(t.Context(), 7); n != 1 {
		t.Errorf("recorded failures = %d, want 1", n)
	}
}
