package broker_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/broker/memory"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/broker"
)

// readySubscriber is a ReadySubscriber driven by a function.
type readySubscriber struct {
	subscribeFunc
	ready func(ctx context.Context, ready func()) error
}

func (s readySubscriber) SubscribeReady(ctx context.Context, _, _ string, _ endpoint.Endpoint, ready func()) error {
	return s.ready(ctx, ready)
}

type checker interface{ Check(context.Context) error }

func TestConsumerNames(t *testing.T) {
	for _, tc := range []struct {
		group string
		opts  []broker.ConsumerOption
		want  string
	}{
		{"", nil, "consumer:orders"},
		{"billing", nil, "consumer:orders:billing"},
		{"billing", []broker.ConsumerOption{broker.WithName("orders-billing")}, "orders-billing"},
		{"", []broker.ConsumerOption{broker.WithName("")}, "consumer:orders"},
	} {
		if got := broker.NewConsumer(blockUntilDone(nil), "orders", tc.group, nopHandler, tc.opts...).Name(); got != tc.want {
			t.Errorf("Name(group %q) = %q, want %q", tc.group, got, tc.want)
		}
	}
}

func TestConsumerStartWaitsForReadySubscriber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		sub := readySubscriber{ready: func(ctx context.Context, ready func()) error {
			<-release
			ready()
			<-ctx.Done()
			return nil
		}}
		h := broker.NewConsumer(sub, "orders", "", nopHandler)
		started := make(chan error, 1)
		go func() { started <- h.Start(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-started:
			t.Fatalf("Start returned %v before the subscription was ready", err)
		default:
		}
		if err := h.(checker).Check(t.Context()); err == nil {
			t.Error("Check while starting = nil")
		}
		close(release)
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		if err := h.(checker).Check(t.Context()); err != nil {
			t.Errorf("Check() = %v", err)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumerReadyFailureFailsStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := readySubscriber{ready: func(context.Context, func()) error { return errBoom }}
		h := broker.NewConsumer(sub, "orders", "", nopHandler)
		a := app.New(app.WithLogger(testLogger(slog.New(slog.DiscardHandler))), app.WithHooks(h))
		err := a.Start(t.Context())
		if !errors.Is(err, errBoom) {
			t.Fatalf("Start() = %v, want %v", err, errBoom)
		}
		if _, ok := <-h.(errReporter).Err(); ok {
			t.Error("start failure also reported through Err")
		}
		if s := a.State(); s != app.StateFailed {
			t.Errorf("state = %v, want failed", s)
		}
	})
}

func TestConsumerReadyBoundedByStartContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := readySubscriber{ready: func(ctx context.Context, _ func()) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		h := broker.NewConsumer(sub, "orders", "", nopHandler)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := h.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start() = %v, want DeadlineExceeded", err)
		}
	})
}

// With the memory broker, a message published right after Start returns
// reaches the consumer: Start waits for the subscriber to join.
func TestConsumerReceivesMessagePublishedAfterStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := memory.New()
		defer b.Close()
		got := make(chan string, 1)
		h := broker.NewConsumer(b, "orders", "billing", func(_ context.Context, req any) (any, error) {
			got <- string(req.(*broker.Message).Body)
			return nil, nil
		})
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = h.Stop(t.Context()) }()
		if err := b.Publish(t.Context(), &broker.Message{Topic: "orders", Body: []byte("o-1")}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case body := <-got:
			if body != "o-1" {
				t.Errorf("body = %q", body)
			}
		default:
			t.Fatal("the message published after Start was dropped")
		}
	})
}

func TestConsumerReadyAfterBrokerClosed(t *testing.T) {
	b := memory.New()
	b.Close()
	h := broker.NewConsumer(b, "orders", "", nopHandler)
	if err := h.Start(t.Context()); err == nil {
		t.Fatal("Start on a closed broker succeeded")
	}
}
