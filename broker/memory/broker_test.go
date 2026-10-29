package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// Compile-time interface checks.
var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
)

// recorder collects message keys in delivery order.
type recorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *recorder) handler(context.Context, any) (any, error) { return nil, nil }

func (r *recorder) record(ctx context.Context, req any) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, req.(*broker.Message).Key)
	return nil, nil
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.keys)
}

// subscription runs Subscribe in the background.
type subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func subscribe(t *testing.T, b *Broker, topic, group string, h endpoint.Endpoint) *subscription {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &subscription{cancel: cancel, done: make(chan struct{})}
	go func() {
		s.err = b.Subscribe(ctx, topic, group, h)
		close(s.done)
	}()
	t.Cleanup(func() {
		if err := s.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
	synctest.Wait() // the subscriber is registered once it blocks
	return s
}

// stop cancels the subscription and returns the Subscribe result.
func (s *subscription) stop() error {
	s.cancel()
	<-s.done
	return s.err
}

func publish(t *testing.T, b *Broker, topic string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := b.Publish(t.Context(), &broker.Message{Topic: topic, Key: k}); err != nil {
			t.Fatalf("Publish(%s) = %v", k, err)
		}
	}
}

func TestGroupDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		var a1, a2, billing, other recorder
		subs := []*subscription{
			subscribe(t, b, "orders", "audit", a1.record),
			subscribe(t, b, "orders", "audit", a2.record),
			subscribe(t, b, "orders", "billing", billing.record),
			subscribe(t, b, "payments", "audit", other.record),
		}
		publish(t, b, "orders", "m0", "m1", "m2", "m3")
		synctest.Wait()

		// One copy per group, round-robin inside a group.
		if got, want := a1.got(), []string{"m0", "m2"}; !slices.Equal(got, want) {
			t.Errorf("audit member 1 got %v, want %v", got, want)
		}
		if got, want := a2.got(), []string{"m1", "m3"}; !slices.Equal(got, want) {
			t.Errorf("audit member 2 got %v, want %v", got, want)
		}
		if got, want := billing.got(), []string{"m0", "m1", "m2", "m3"}; !slices.Equal(got, want) {
			t.Errorf("billing got %v, want %v", got, want)
		}
		if got := other.got(); len(got) != 0 {
			t.Errorf("other topic got %v, want nothing", got)
		}
		for _, s := range subs {
			if err := s.stop(); err != nil {
				t.Errorf("Subscribe() = %v, want nil", err)
			}
		}
	})
}

func TestBroadcastDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		var r1, r2, grouped recorder
		subs := []*subscription{
			subscribe(t, b, "events", "", r1.record),
			subscribe(t, b, "events", "", r2.record),
			subscribe(t, b, "events", "g", grouped.record),
		}
		publish(t, b, "events", "a", "b")
		synctest.Wait()
		for i, r := range []*recorder{&r1, &r2, &grouped} {
			if got, want := r.got(), []string{"a", "b"}; !slices.Equal(got, want) {
				t.Errorf("subscriber %d got %v, want %v", i, got, want)
			}
		}
		for _, s := range subs {
			if err := s.stop(); err != nil {
				t.Errorf("Subscribe() = %v, want nil", err)
			}
		}
	})
}

func TestPublishWithoutSubscribers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		publish(t, b, "nobody", "a")
		var r recorder
		s := subscribe(t, b, "nobody", "", r.record)
		synctest.Wait()
		if got := r.got(); len(got) != 0 {
			t.Fatalf("late subscriber got %v, want nothing", got)
		}
		if err := s.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestPublishValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		tests := []struct {
			name string
			ctx  context.Context
			msg  *broker.Message
			is   error
		}{
			{"nil message", t.Context(), nil, nil},
			{"empty topic", t.Context(), &broker.Message{}, nil},
			{"canceled context", canceled, &broker.Message{Topic: "t"}, context.Canceled},
		}
		for _, tt := range tests {
			err := b.Publish(tt.ctx, tt.msg)
			if err == nil {
				t.Errorf("%s: Publish() = nil, want error", tt.name)
				continue
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Errorf("%s: Publish() = %v, want %v", tt.name, err, tt.is)
			}
		}
		if err := b.Subscribe(t.Context(), "", "", (&recorder{}).handler); err == nil {
			t.Error("Subscribe with empty topic succeeded")
		}
		if err := b.Subscribe(t.Context(), "t", "", nil); err == nil {
			t.Error("Subscribe with nil handler succeeded")
		}
	})
}

func TestMessagesAreCopied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		got := make(chan *broker.Message, 2)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		h := func(_ context.Context, req any) (any, error) {
			<-release
			got <- req.(*broker.Message)
			return nil, nil
		}
		s1 := subscribe(t, b, "t", "", h)
		s2 := subscribe(t, b, "t", "", h)
		msg := &broker.Message{Topic: "t", Key: "k", Header: metadata.Pairs("a", "1"), Body: []byte("body")}
		if err := b.Publish(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
		msg.Header.Set("a", "changed")
		msg.Body[0] = 'X'
		unblock()
		m1, m2 := <-got, <-got
		if m1 == m2 || m1 == msg {
			t.Fatal("subscribers share message values")
		}
		for _, m := range []*broker.Message{m1, m2} {
			if m.Header.Get("a") != "1" || string(m.Body) != "body" || m.Key != "k" || m.Topic != "t" {
				t.Fatalf("delivered message %+v was affected by the publisher", m)
			}
		}
		if err := s1.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
		if err := s2.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestAckNackAndDeadLetter(t *testing.T) {
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	tests := []struct {
		name            string
		opts            []Option
		failures        int   // number of leading attempts that fail
		err             error // error returned by failing attempts
		wantAttempts    int
		wantDeadLetters int
	}{
		{"ack", nil, 0, nil, 1, 0},
		{"retryable then ack", nil, 2, unavailable, 3, 0},
		{"retryable exhausts default redeliveries", nil, 100, unavailable, 4, 1},
		{"retryable exhausts custom redeliveries", []Option{WithMaxRedeliveries(1)}, 100, unavailable, 2, 1},
		{"zero redeliveries", []Option{WithMaxRedeliveries(0)}, 100, unavailable, 1, 1},
		{"negative redeliveries", []Option{WithMaxRedeliveries(-5)}, 100, unavailable, 1, 1},
		{"plain error is not retryable", nil, 100, errors.New("bad"), 1, 1},
		{"non-retryable code", nil, 100, errorx.New(errorx.InvalidArgument, "BAD", "bad"), 1, 1},
		{"explicitly retryable", []Option{WithMaxRedeliveries(2)}, 100,
			errorx.New(errorx.Internal, "X", "x").WithRetryable(true), 3, 1},
		{"explicitly non-retryable", nil, 100, unavailable.WithRetryable(false), 1, 1},
		{"wrapped retryable", []Option{WithMaxRedeliveries(1)}, 100, fmt.Errorf("wrap: %w", unavailable), 2, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type dead struct {
					msg *broker.Message
					err error
				}
				var deads []dead
				opts := append(slices.Clone(tt.opts), WithDeadLetter(func(msg *broker.Message, err error) {
					deads = append(deads, dead{msg, err})
				}))
				b := New(opts...)
				attempts := 0
				h := func(_ context.Context, req any) (any, error) {
					attempts++
					if attempts <= tt.failures {
						return nil, tt.err
					}
					return nil, nil
				}
				s := subscribe(t, b, "t", "g", h)
				publish(t, b, "t", "k")
				synctest.Wait()
				if attempts != tt.wantAttempts {
					t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
				}
				if len(deads) != tt.wantDeadLetters {
					t.Fatalf("dead letters = %d, want %d", len(deads), tt.wantDeadLetters)
				}
				if len(deads) == 1 && (deads[0].msg.Key != "k" || !errors.Is(deads[0].err, tt.err)) {
					t.Errorf("dead letter = %+v, want key k and error %v", deads[0], tt.err)
				}
				if err := s.stop(); err != nil {
					t.Errorf("Subscribe() = %v, want nil", err)
				}
			})
		})
	}
}

func TestDeadLetterIsOptional(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		var calls atomic.Int32
		s := subscribe(t, b, "t", "", func(context.Context, any) (any, error) {
			calls.Add(1)
			return nil, errors.New("bad")
		})
		publish(t, b, "t", "a", "b")
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("handler calls = %d, want 2", got)
		}
		if err := s.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestPublishDoesNotWaitForHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(withQueueSize(4))
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var r recorder
		s := subscribe(t, b, "t", "", func(ctx context.Context, req any) (any, error) {
			<-release
			return r.record(ctx, req)
		})
		// One message is in flight and four are queued; none of these block.
		publish(t, b, "t", "0", "1", "2", "3", "4")
		unblock()
		synctest.Wait()
		if got, want := r.got(), []string{"0", "1", "2", "3", "4"}; !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if err := s.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestPublishBlocksWhenQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(withQueueSize(1))
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var r recorder
		s := subscribe(t, b, "t", "g", func(ctx context.Context, req any) (any, error) {
			<-release
			return r.record(ctx, req)
		})
		publish(t, b, "t", "in-flight", "queued")

		// The queue is full: Publish waits until its context expires.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		begin := time.Now()
		err := b.Publish(ctx, &broker.Message{Topic: "t", Key: "timeout"})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Publish() = %v, want DeadlineExceeded", err)
		}
		if got := time.Since(begin); got != time.Second {
			t.Fatalf("Publish blocked for %v, want 1s", got)
		}

		// A blocked Publish proceeds as soon as the handler makes room.
		published := make(chan error, 1)
		go func() { published <- b.Publish(t.Context(), &broker.Message{Topic: "t", Key: "late"}) }()
		synctest.Wait()
		select {
		case err := <-published:
			t.Fatalf("Publish returned %v while the queue was full", err)
		default:
		}
		unblock()
		if err := <-published; err != nil {
			t.Fatalf("Publish() = %v", err)
		}
		synctest.Wait()
		if got, want := r.got(), []string{"in-flight", "queued", "late"}; !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if err := s.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(withQueueSize(1))
		handlerCanceled := make(chan struct{})
		s := subscribe(t, b, "t", "g", func(ctx context.Context, _ any) (any, error) {
			<-ctx.Done()
			close(handlerCanceled)
			return nil, ctx.Err()
		})
		publish(t, b, "t", "in-flight", "queued")
		blocked := make(chan error, 1)
		go func() { blocked <- b.Publish(t.Context(), &broker.Message{Topic: "t"}) }()
		synctest.Wait()

		if err := b.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		<-handlerCanceled
		if err := s.stop(); err != nil {
			t.Fatalf("Subscribe() after Close = %v, want nil", err)
		}
		if err := <-blocked; !errors.Is(err, errClosed) {
			t.Fatalf("blocked Publish() = %v, want closed error", err)
		}
		if err := b.Publish(t.Context(), &broker.Message{Topic: "t"}); !errors.Is(err, errClosed) {
			t.Fatalf("Publish() after Close = %v, want closed error", err)
		}
		if err := b.Subscribe(t.Context(), "t", "", (&recorder{}).handler); err != nil {
			t.Fatalf("Subscribe() after Close = %v, want nil", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("second Close() = %v", err)
		}
		s.cancel()
	})
}

func TestLeavingGroupMemberHandsOverMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		var stay recorder
		var leaverCalls atomic.Int32
		leaver := subscribe(t, b, "t", "g", func(ctx context.Context, _ any) (any, error) {
			leaverCalls.Add(1)
			<-ctx.Done() // never settles a message
			return nil, ctx.Err()
		})
		stayer := subscribe(t, b, "t", "g", stay.record)
		// Round-robin: the leaver receives m0 (in flight) and m2 (queued).
		publish(t, b, "t", "m0", "m1", "m2", "m3")
		synctest.Wait()
		if got, want := stay.got(), []string{"m1", "m3"}; !slices.Equal(got, want) {
			t.Fatalf("before leave: got %v, want %v", got, want)
		}
		if err := leaver.stop(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		got := stay.got()
		slices.Sort(got)
		if want := []string{"m0", "m1", "m2", "m3"}; !slices.Equal(got, want) {
			t.Fatalf("after leave: got %v, want %v", got, want)
		}
		if leaverCalls.Load() != 1 {
			t.Fatalf("leaver handled %d messages, want 1", leaverCalls.Load())
		}
		// The group keeps receiving through the remaining member only.
		publish(t, b, "t", "m4")
		synctest.Wait()
		if got := stay.got(); got[len(got)-1] != "m4" {
			t.Fatalf("after leave: last message %q, want m4", got[len(got)-1])
		}
		if err := stayer.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestLeaveWithBackloggedGroupReturnsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(withQueueSize(1))
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var stay recorder
		stayer := subscribe(t, b, "t", "g", func(ctx context.Context, req any) (any, error) {
			<-release
			return stay.record(ctx, req)
		})
		leaver := subscribe(t, b, "t", "g", func(ctx context.Context, _ any) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		// Both members end up with a message in flight and one queued.
		publish(t, b, "t", "m0", "m1", "m2", "m3")
		synctest.Wait()

		// The remaining member is backlogged, yet leaving must not wait for it.
		if err := leaver.stop(); err != nil {
			t.Fatal(err)
		}
		unblock()
		synctest.Wait()
		got := stay.got()
		slices.Sort(got)
		if want := []string{"m0", "m1", "m2", "m3"}; !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if err := stayer.stop(); err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	})
}

func TestNonRetryableErrorWhileStoppingIsDeadLettered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dead []string
		var mu sync.Mutex
		b := New(WithDeadLetter(func(msg *broker.Message, _ error) {
			mu.Lock()
			defer mu.Unlock()
			dead = append(dead, msg.Key)
		}))
		handling := make(chan struct{})
		s := subscribe(t, b, "t", "g", func(ctx context.Context, _ any) (any, error) {
			close(handling)
			<-ctx.Done()
			// The handler rejects the message explicitly after stop began.
			return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad message")
		})
		publish(t, b, "t", "m0")
		<-handling
		if err := s.stop(); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(dead, []string{"m0"}) {
			t.Fatalf("dead letters = %v, want [m0]", dead)
		}
	})
}

func TestLastGroupMemberLeaving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		block := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(block) })
		defer unblock()
		s := subscribe(t, b, "t", "g", func(ctx context.Context, _ any) (any, error) {
			<-block
			return nil, nil
		})
		publish(t, b, "t", "a", "b")
		s.cancel()
		unblock()
		if err := s.stop(); err != nil {
			t.Fatal(err)
		}
		// The group is gone, so publishing does not block or fail.
		publish(t, b, "t", "c")
		b.mu.Lock()
		n := len(b.topics)
		b.mu.Unlock()
		if n != 0 {
			t.Fatalf("topics left after last subscriber: %d", n)
		}
	})
}

func TestBroadcastLeaveUnblocksPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(withQueueSize(1))
		block := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(block) })
		defer unblock()
		s := subscribe(t, b, "t", "", func(ctx context.Context, _ any) (any, error) {
			<-block
			return nil, nil
		})
		publish(t, b, "t", "in-flight", "queued")
		published := make(chan error, 1)
		go func() { published <- b.Publish(t.Context(), &broker.Message{Topic: "t"}) }()
		synctest.Wait()
		s.cancel()
		unblock()
		if err := <-published; err != nil {
			t.Fatalf("Publish() = %v, want nil after the subscriber left", err)
		}
		if err := s.stop(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConcurrentPublishAndSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			publishers = 8
			perPub     = 50
			members    = 3
		)
		b := New(withQueueSize(4))
		var grouped, broadcast atomic.Int64
		var subs []*subscription
		for range members {
			subs = append(subs, subscribe(t, b, "t", "g", func(context.Context, any) (any, error) {
				grouped.Add(1)
				return nil, nil
			}))
		}
		subs = append(subs, subscribe(t, b, "t", "", func(context.Context, any) (any, error) {
			broadcast.Add(1)
			return nil, nil
		}))
		var wg sync.WaitGroup
		for p := range publishers {
			wg.Go(func() {
				for i := range perPub {
					msg := &broker.Message{Topic: "t", Key: fmt.Sprintf("%d-%d", p, i)}
					if err := b.Publish(t.Context(), msg); err != nil {
						t.Errorf("Publish() = %v", err)
					}
				}
			})
		}
		wg.Wait()
		synctest.Wait()
		const total = publishers * perPub
		if got := grouped.Load(); got != total {
			t.Errorf("group handled %d messages, want %d", got, total)
		}
		if got := broadcast.Load(); got != total {
			t.Errorf("broadcast subscriber handled %d messages, want %d", got, total)
		}
		for _, s := range subs {
			if err := s.stop(); err != nil {
				t.Errorf("Subscribe() = %v, want nil", err)
			}
		}
	})
}

func TestWithConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New()
		got := make(chan metadata.Info, 1)
		h := broker.NewConsumer(b, "orders", "g", func(ctx context.Context, _ any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			got <- info
			return nil, nil
		})
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		msg := &broker.Message{Topic: "orders", Header: metadata.Pairs("x-id", "42")}
		if err := b.Publish(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
		info := <-got
		if info.Protocol != "broker" || info.Operation != "orders" || info.Header.Get("x-id") != "42" {
			t.Fatalf("Info = %+v", info)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
	})
}
