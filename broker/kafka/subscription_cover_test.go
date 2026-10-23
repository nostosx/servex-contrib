package kafka

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/errorx"
)

func TestSleep(t *testing.T) {
	tests := []struct {
		name     string
		d        time.Duration
		canceled bool
		want     bool
		elapsed  time.Duration
	}{
		{"no delay", 0, false, true, 0},
		{"negative delay", -time.Second, false, true, 0},
		{"no delay after the end", 0, true, false, 0},
		{"full delay", time.Minute, false, true, time.Minute},
		{"delay cut short", time.Minute, true, false, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				switch {
				case !tt.canceled:
				case tt.d <= 0:
					cancel()
				default:
					// End the context half way through the delay.
					time.AfterFunc(tt.d/2, cancel)
				}
				start := time.Now()
				if got := sleep(ctx, tt.d); got != tt.want {
					t.Fatalf("sleep(%v) = %v, want %v", tt.d, got, tt.want)
				}
				if elapsed := time.Since(start); elapsed != tt.elapsed {
					t.Errorf("sleep(%v) took %v, want %v", tt.d, elapsed, tt.elapsed)
				}
			})
		})
	}
}

// handleSub returns a subscription for calling handle directly, producing
// dead letters through producer.
func handleSub(producer *kgo.Client, opts ...Option) *subscription {
	o := options{maxRedeliveries: defaultMaxRedeliveries}
	for _, opt := range opts {
		opt(&o)
	}
	return &subscription{b: &Broker{producer: producer, opts: o}, topic: "orders", group: "g"}
}

func TestHandleStopsDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s := handleSub(nil, WithDeadLetterTopic("dead"), WithBackoff(func(int) time.Duration { return time.Hour }))
		calls := 0
		h := func(context.Context, any) (any, error) {
			calls++
			go func() {
				time.Sleep(time.Minute)
				cancel()
			}()
			return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
		}
		// The record is neither settled nor dead lettered (the nil producer
		// would panic), so that it is redelivered.
		ok, err := s.handle(ctx, h, &kgo.Record{Topic: "orders"})
		if ok || err != nil {
			t.Fatalf("handle() = %v, %v, want false, nil", ok, err)
		}
		if calls != 1 {
			t.Errorf("handler calls = %d, want 1", calls)
		}
	})
}

// closedProducer returns a client that fails every produce request.
func closedProducer(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
	return cl
}

func TestHandleDeadLetterFailure(t *testing.T) {
	rejected := errors.New("bad message")
	h := func(context.Context, any) (any, error) { return nil, rejected }
	s := handleSub(closedProducer(t), WithDeadLetterTopic("dead"))

	t.Run("running", func(t *testing.T) {
		ok, err := s.handle(t.Context(), h, &kgo.Record{Topic: "orders", Value: []byte("v")})
		if ok {
			t.Fatal("handle() settled a record that could not be dead lettered")
		}
		if errorx.CodeOf(err) != errorx.FailedPrecondition || !errors.Is(err, kgo.ErrClientClosed) {
			t.Fatalf("handle() error = %v, want FailedPrecondition caused by the closed client", err)
		}
	})
	t.Run("stopping", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		// While stopping, the record is left unsettled for redelivery instead
		// of failing the subscription.
		ok, err := s.handle(ctx, h, &kgo.Record{Topic: "orders", Value: []byte("v")})
		if ok || err != nil {
			t.Fatalf("handle() = %v, %v, want false, nil", ok, err)
		}
	})
}
