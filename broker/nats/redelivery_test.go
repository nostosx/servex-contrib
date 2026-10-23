package nats

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
)

// deadLetters records dead lettered messages.
type deadLetters struct {
	mu   sync.Mutex
	msgs []*broker.Message
}

func (d *deadLetters) add(msg *broker.Message, _ error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, msg)
}

func (d *deadLetters) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.msgs)
}

func TestInterruptedDeliveriesDoNotCount(t *testing.T) {
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	for _, tt := range []struct {
		name            string
		maxRedeliveries int
		interruptions   int
	}{
		{"no redeliveries", 0, 1},
		{"one redelivery", 1, 1},
		{"several interruptions", 2, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var dead deadLetters
			f := newFixture(t, WithMaxRedeliveries(tt.maxRedeliveries), WithAckWait(time.Minute),
				WithBackoff(func(int) time.Duration { return time.Millisecond }), WithDeadLetter(dead.add))
			f.publish(t, "orders", "m0")
			for range tt.interruptions {
				handling := make(chan struct{})
				var once sync.Once
				s := f.subscribe(t, "orders", "g", func(ctx context.Context, _ any) (any, error) {
					once.Do(func() { close(handling) })
					<-ctx.Done()
					return nil, ctx.Err()
				})
				select {
				case <-handling:
				case <-time.After(waitTimeout):
					t.Fatal("message not delivered")
				}
				if err := s.stop(t); err != nil {
					t.Fatalf("Subscribe() = %v", err)
				}
			}
			var attempts atomic.Int32
			s := f.subscribe(t, "orders", "g", func(context.Context, any) (any, error) {
				attempts.Add(1)
				return nil, unavailable
			})
			eventually(t, "dead letter", func() bool { return dead.len() == 1 })
			eventually(t, "message terminated", func() bool { return f.settledConsumer(t, "orders", "g") })
			time.Sleep(50 * time.Millisecond) // no further redelivery
			if err := s.stop(t); err != nil {
				t.Fatal(err)
			}
			if got, want := int(attempts.Load()), tt.maxRedeliveries+1; got != want {
				t.Fatalf("handler failures after interruptions = %d, want %d", got, want)
			}
			if got := dead.len(); got != 1 {
				t.Fatalf("dead letters = %d, want 1", got)
			}
		})
	}
}

func TestRedeliveryCountSharedByGroupMembers(t *testing.T) {
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	var dead deadLetters
	opts := []Option{
		WithMaxRedeliveries(2), WithAckWait(time.Minute), WithDeadLetter(dead.add),
		WithBackoff(func(int) time.Duration { return 200 * time.Millisecond }),
	}
	// The first member records one failure; the backoff is requested once the
	// failure was recorded.
	failed := make(chan struct{})
	var once sync.Once
	f := newFixture(t, append(slices.Clone(opts), WithBackoff(func(int) time.Duration {
		once.Do(func() { close(failed) })
		return 200 * time.Millisecond
	}))...)
	s := f.subscribe(t, "orders", "g", func(context.Context, any) (any, error) {
		return nil, unavailable
	})
	f.publish(t, "orders", "m0")
	<-failed
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}

	// Another member, here in another Broker as in another process, sees the
	// failure recorded by the first one.
	ready := make(chan struct{}, 1)
	other := New(f.js, append(opts, withSubscribed(func(string, string) { ready <- struct{}{} }))...)
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- other.Subscribe(ctx, "orders", "g", func(context.Context, any) (any, error) {
			attempts.Add(1)
			return nil, unavailable
		})
	}()
	<-ready
	eventually(t, "dead letter", func() bool { return dead.len() == 1 })
	time.Sleep(300 * time.Millisecond) // no further redelivery
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts of the second member = %d, want 2", got)
	}
}

func TestBroadcastRedeliveries(t *testing.T) {
	var dead deadLetters
	f := newFixture(t, WithMaxRedeliveries(2), WithDeadLetter(dead.add),
		WithBackoff(func(int) time.Duration { return time.Millisecond }))
	var attempts atomic.Int32
	s := f.subscribe(t, "orders", "", func(context.Context, any) (any, error) {
		attempts.Add(1)
		return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
	})
	f.publish(t, "orders", "m0")
	eventually(t, "dead letter", func() bool { return dead.len() == 1 })
	time.Sleep(50 * time.Millisecond)
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestRedeliveryCountFallsBackWithoutBucket(t *testing.T) {
	// An invalid bucket name makes the counter store unusable; the delivery
	// count still bounds the retries.
	var dead deadLetters
	f := newFixture(t, WithMaxRedeliveries(2), WithDeadLetter(dead.add), WithRedeliveryBucket("bad name"),
		WithBackoff(func(int) time.Duration { return time.Millisecond }))
	var attempts atomic.Int32
	s := f.subscribe(t, "orders", "g", func(context.Context, any) (any, error) {
		attempts.Add(1)
		return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
	})
	f.publish(t, "orders", "m0")
	eventually(t, "dead letter", func() bool { return dead.len() == 1 })
	eventually(t, "message terminated", func() bool { return f.settledConsumer(t, "orders", "g") })
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestRedeliveryCounterIsCleared(t *testing.T) {
	f := newFixture(t, WithBackoff(func(int) time.Duration { return time.Millisecond }))
	var attempts atomic.Int32
	s := f.subscribe(t, "orders", "g", func(context.Context, any) (any, error) {
		if attempts.Add(1) == 1 {
			return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
		}
		return nil, nil
	})
	f.publish(t, "orders", "m0")
	eventually(t, "message settled", func() bool { return attempts.Load() == 2 && f.settledConsumer(t, "orders", "g") })
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	kv, err := f.js.KeyValue(t.Context(), DefaultRedeliveryBucket)
	if err != nil {
		t.Fatalf("KeyValue() = %v", err)
	}
	keys, err := kv.ListKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for k := range keys.Keys() {
		t.Errorf("counter %q left after the message was settled", k)
	}
}

// failingPublish makes PublishMsg fail with err.
type failingPublish struct {
	jetstream.JetStream
	err error
}

func (p failingPublish) PublishMsg(context.Context, *natsgo.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, p.err
}

func TestNoRespondersIsTransient(t *testing.T) {
	for _, err := range []error{natsgo.ErrNoResponders, jetstream.ErrNoStreamResponse,
		errors.Join(errors.New("request"), natsgo.ErrNoResponders)} {
		got := classify(t.Context(), "op", err)
		if errorx.CodeOf(got) != errorx.Unavailable || !errorx.IsRetryable(got) || !errors.Is(got, err) {
			t.Errorf("classify(%v) = %v, want retryable Unavailable", err, got)
		}
	}

	f := newFixture(t)
	b := New(failingPublish{JetStream: f.js, err: jetstream.ErrNoStreamResponse})
	// The subject is bound to a stream that did not answer, for example while
	// it elects a leader.
	err := b.Publish(t.Context(), &broker.Message{Topic: "orders"})
	if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
		t.Errorf("Publish(bound, no response) = %v, want retryable Unavailable", err)
	}
	// No stream is bound to the subject.
	err = b.Publish(t.Context(), &broker.Message{Topic: "unbound"})
	if errorx.CodeOf(err) != errorx.FailedPrecondition || errorx.IsRetryable(err) {
		t.Errorf("Publish(unbound) = %v, want non-retryable FailedPrecondition", err)
	}
}
