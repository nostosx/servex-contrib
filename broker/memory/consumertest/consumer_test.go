package broker_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

var errBoom = errors.New("boom")

// subscribeFunc adapts a function to broker.Subscriber.
type subscribeFunc func(ctx context.Context, topic, group string, h endpoint.Endpoint) error

func (f subscribeFunc) Subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	return f(ctx, topic, group, h)
}

// errReporter is the optional runtime-error interface watched by app.
type errReporter interface{ Err() <-chan error }

// blockUntilDone returns a subscriber that blocks until its context ends.
func blockUntilDone(ret error) subscribeFunc {
	return func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
		<-ctx.Done()
		return ret
	}
}

func nopHandler(context.Context, any) (any, error) { return nil, nil }

func TestConsumerName(t *testing.T) {
	h := broker.NewConsumer(blockUntilDone(nil), "orders", "g", nopHandler)
	if got, want := h.Name(), "consumer:orders:g"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
	if _, ok := h.(errReporter); !ok {
		t.Fatal("consumer does not implement Err() <-chan error")
	}
}

func TestConsumerLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			gotTopic, gotGroup string
			running, canceled  bool
		)
		sub := subscribeFunc(func(ctx context.Context, topic, group string, _ endpoint.Endpoint) error {
			gotTopic, gotGroup, running = topic, group, true
			<-ctx.Done()
			canceled = true
			return nil
		})
		h := broker.NewConsumer(sub, "orders", "billing", nopHandler)

		if err := h.Start(t.Context()); err != nil {
			t.Fatalf("Start() = %v", err)
		}
		synctest.Wait()
		if !running || gotTopic != "orders" || gotGroup != "billing" {
			t.Fatalf("Subscribe not running with topic/group: running=%v topic=%q group=%q", running, gotTopic, gotGroup)
		}
		if canceled {
			t.Fatal("subscription canceled before Stop")
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
		if !canceled {
			t.Fatal("Stop returned before Subscribe observed cancellation")
		}
		if _, ok := <-h.(errReporter).Err(); ok {
			t.Fatal("Err() channel not closed after a clean stop")
		}
		// Stop is idempotent.
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("second Stop() = %v", err)
		}
	})
}

func TestConsumerRunContextIgnoresStartCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		var gotValue any
		var ended bool
		sub := subscribeFunc(func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
			gotValue = ctx.Value(key{})
			<-ctx.Done()
			ended = true
			return nil
		})
		h := broker.NewConsumer(sub, "t", "", nopHandler)
		startCtx, cancel := context.WithCancel(context.WithValue(t.Context(), key{}, "v"))
		if err := h.Start(startCtx); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if ended {
			t.Fatal("canceling the Start context stopped the subscription")
		}
		if gotValue != "v" {
			t.Fatalf("run context value = %v, want %q", gotValue, "v")
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumerStopWaitsForSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const drain = 5 * time.Second
		sub := subscribeFunc(func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
			<-ctx.Done()
			time.Sleep(drain)
			return nil
		})
		h := broker.NewConsumer(sub, "t", "", nopHandler)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		begin := time.Now()
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
		if got := time.Since(begin); got != drain {
			t.Fatalf("Stop returned after %v, want %v", got, drain)
		}
	})
}

func TestConsumerStopTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		sub := subscribeFunc(func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
			<-release
			return nil
		})
		h := broker.NewConsumer(sub, "t", "", nopHandler)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := h.Stop(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop() = %v, want DeadlineExceeded", err)
		}
		close(release)
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() after release = %v", err)
		}
	})
}

func TestConsumerErrReportsSubscribeFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := subscribeFunc(func(context.Context, string, string, endpoint.Endpoint) error {
			return errBoom
		})
		h := broker.NewConsumer(sub, "orders", "", nopHandler)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		err, ok := <-h.(errReporter).Err()
		if !ok || !errors.Is(err, errBoom) {
			t.Fatalf("Err() = %v, %v; want boom", err, ok)
		}
		if !strings.Contains(err.Error(), "consumer:orders") {
			t.Fatalf("error %q does not name the consumer", err)
		}
		if _, ok := <-h.(errReporter).Err(); ok {
			t.Fatal("Err() channel not closed after Subscribe returned")
		}
		// The error was already delivered, so Stop does not repeat it.
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
	})
}

func TestConsumerErrReportsUnexpectedEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Subscribe returning nil before Stop (e.g. the broker was closed) must
		// not leave the App running and ready with nothing consumed.
		sub := subscribeFunc(func(context.Context, string, string, endpoint.Endpoint) error {
			return nil
		})
		h := broker.NewConsumer(sub, "orders", "", nopHandler)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		err, ok := <-h.(errReporter).Err()
		if !ok || err == nil || !strings.Contains(err.Error(), "consumer:orders") {
			t.Fatalf("Err() = %v, %v; want an unexpected-end error naming the consumer", err, ok)
		}
	})
}

func TestConsumerStopResult(t *testing.T) {
	tests := []struct {
		name    string
		ret     error
		wantErr error
	}{
		{"nil", nil, nil},
		{"context canceled is a clean stop", context.Canceled, nil},
		{"wrapped context canceled is a clean stop", errors.Join(errors.New("x"), context.Canceled), nil},
		{"other error is returned", errBoom, errBoom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := broker.NewConsumer(blockUntilDone(tt.ret), "t", "", nopHandler)
				if err := h.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				err := h.Stop(t.Context())
				if tt.wantErr == nil {
					if err != nil {
						t.Fatalf("Stop() = %v, want nil", err)
					}
					return
				}
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Stop() = %v, want %v", err, tt.wantErr)
				}
			})
		})
	}
}

func TestConsumerStartErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := broker.NewConsumer(blockUntilDone(nil), "t", "", nopHandler)
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() before Start = %v", err)
		}

		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if err := h.Start(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start(canceled) = %v, want context.Canceled", err)
		}

		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := h.Start(t.Context()); err == nil || !strings.HasPrefix(err.Error(), "broker: ") {
			t.Fatalf("second Start() = %v, want broker error", err)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := h.Start(t.Context()); err == nil {
			t.Fatal("Start() after Stop succeeded")
		}
	})
}

func TestConsumerMetadataAndMiddleware(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var trace []string
		record := func(s string) {
			mu.Lock()
			defer mu.Unlock()
			trace = append(trace, s)
		}
		mw := func(name string) endpoint.Middleware {
			return func(next endpoint.Endpoint) endpoint.Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					if info, ok := metadata.FromContext(ctx); !ok || info.Protocol != "broker" {
						t.Errorf("middleware %s: Info missing: %+v", name, info)
					}
					record(name + ">")
					resp, err := next(ctx, req)
					record("<" + name)
					return resp, err
				}
			}
		}
		var gotInfo metadata.Info
		handler := func(ctx context.Context, req any) (any, error) {
			gotInfo, _ = metadata.FromContext(ctx)
			record("handler")
			if req.(*broker.Message).Key == "bad" {
				return nil, errBoom
			}
			return "ignored", nil
		}

		results := make(chan error, 2)
		sub := subscribeFunc(func(ctx context.Context, topic, _ string, h endpoint.Endpoint) error {
			for _, key := range []string{"good", "bad"} {
				msg := &broker.Message{Topic: topic, Key: key, Header: metadata.Pairs("trace-id", "abc")}
				_, err := h(ctx, msg)
				results <- err
			}
			<-ctx.Done()
			return nil
		})
		h := broker.NewConsumer(sub, "orders", "g", handler,
			broker.WithMiddleware(mw("a"), nil),
			broker.WithMiddleware(mw("b")),
		)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := <-results; err != nil {
			t.Fatalf("ack: handler error = %v, want nil", err)
		}
		if err := <-results; !errors.Is(err, errBoom) {
			t.Fatalf("nack: handler error = %v, want boom", err)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}

		want := metadata.Info{
			Kind:      metadata.KindServer,
			Protocol:  "broker",
			Operation: "orders",
			Header:    metadata.Pairs("trace-id", "abc"),
		}
		if gotInfo.Kind != want.Kind || gotInfo.Protocol != want.Protocol ||
			gotInfo.Operation != want.Operation || gotInfo.Header.Get("trace-id") != "abc" {
			t.Fatalf("Info = %+v, want %+v", gotInfo, want)
		}
		wantTrace := []string{"a>", "b>", "handler", "<b", "<a", "a>", "b>", "handler", "<b", "<a"}
		if !slices.Equal(trace, wantTrace) {
			t.Fatalf("call order = %v, want %v", trace, wantTrace)
		}
	})
}

func TestConsumerNonMessageRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		done := make(chan metadata.Info, 1)
		handler := func(ctx context.Context, _ any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			done <- info
			return nil, nil
		}
		sub := subscribeFunc(func(ctx context.Context, _, _ string, h endpoint.Endpoint) error {
			if _, err := h(ctx, "not a message"); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		})
		h := broker.NewConsumer(sub, "t", "", handler)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		info := <-done
		if info.Operation != "t" || info.Header != nil {
			t.Fatalf("Info = %+v, want operation t and nil header", info)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumerFailureStopsApp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := subscribeFunc(func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
			select {
			case <-time.After(time.Second):
				return errBoom
			case <-ctx.Done():
				return nil
			}
		})
		a := app.New(
			app.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
			app.WithHooks(broker.NewConsumer(sub, "orders", "g", nopHandler)),
		)
		err := a.Run(t.Context())
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run() = %v, want boom", err)
		}
		if got := a.State(); got != app.StateFailed {
			t.Fatalf("State() = %v, want %v", got, app.StateFailed)
		}
	})
}

func TestConsumerStopsWithApp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stopped bool
		sub := subscribeFunc(func(ctx context.Context, _, _ string, _ endpoint.Endpoint) error {
			<-ctx.Done()
			stopped = true
			return nil
		})
		a := app.New(
			app.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
			app.WithHooks(broker.NewConsumer(sub, "orders", "g", nopHandler)),
		)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		if err := a.Run(ctx); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		if !stopped {
			t.Fatal("subscription still running after App.Run returned")
		}
	})
}
