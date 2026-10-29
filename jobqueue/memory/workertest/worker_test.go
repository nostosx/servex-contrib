package jobqueue_test

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
)

var errBoom = errors.New("boom")

// runFunc adapts a function to jobqueue.Runner.
type runFunc func(ctx context.Context, handlers map[string]endpoint.Endpoint) error

func (f runFunc) Run(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
	return f(ctx, handlers)
}

// errReporter is the optional runtime-error interface watched by app.
type errReporter interface{ Err() <-chan error }

func blockUntilDone(ret error) runFunc {
	return func(ctx context.Context, _ map[string]endpoint.Endpoint) error {
		<-ctx.Done()
		return ret
	}
}

func nopHandler(context.Context, any) (any, error) { return nil, nil }

func TestWorkerName(t *testing.T) {
	h := jobqueue.NewWorker(blockUntilDone(nil), nil)
	if got := h.Name(); got != "worker" {
		t.Fatalf("Name() = %q, want %q", got, "worker")
	}
	if _, ok := h.(errReporter); !ok {
		t.Fatal("worker does not implement Err() <-chan error")
	}
}

func TestWorkerLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running, canceled bool
		var gotTypes []string
		r := runFunc(func(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
			running = true
			gotTypes = slices.Sorted(maps.Keys(handlers))
			<-ctx.Done()
			time.Sleep(time.Second) // drain
			canceled = true
			return nil
		})
		handlers := map[string]endpoint.Endpoint{"email": nopHandler, "sms": nopHandler, "nil": nil}
		h := jobqueue.NewWorker(r, handlers)
		handlers["late"] = nopHandler // the worker keeps its own copy

		if err := h.Start(t.Context()); err != nil {
			t.Fatalf("Start() = %v", err)
		}
		synctest.Wait()
		if !running {
			t.Fatal("Run not started")
		}
		if want := []string{"email", "sms"}; !slices.Equal(gotTypes, want) {
			t.Fatalf("handler types = %v, want %v", gotTypes, want)
		}
		begin := time.Now()
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
		if !canceled || time.Since(begin) != time.Second {
			t.Fatalf("Stop did not wait for Run: canceled=%v waited=%v", canceled, time.Since(begin))
		}
		if _, ok := <-h.(errReporter).Err(); ok {
			t.Fatal("Err() channel not closed after a clean stop")
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("second Stop() = %v", err)
		}
		if err := h.Start(t.Context()); err == nil || !strings.HasPrefix(err.Error(), "jobqueue: ") {
			t.Fatalf("Start() after Stop = %v, want jobqueue error", err)
		}
	})
}

func TestWorkerStopBeforeStartAndCanceledStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := jobqueue.NewWorker(blockUntilDone(nil), nil)
		if err := h.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() before Start = %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := h.Start(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start(canceled) = %v", err)
		}
	})
}

func TestWorkerStopTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		h := jobqueue.NewWorker(runFunc(func(context.Context, map[string]endpoint.Endpoint) error {
			<-release
			return nil
		}), nil)
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := h.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop() = %v, want DeadlineExceeded", err)
		}
		close(release)
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWorkerRunErrors(t *testing.T) {
	t.Run("before stop goes to Err", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := jobqueue.NewWorker(runFunc(func(context.Context, map[string]endpoint.Endpoint) error {
				return errBoom
			}), nil)
			if err := h.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err, ok := <-h.(errReporter).Err(); !ok || !errors.Is(err, errBoom) {
				t.Fatalf("Err() = %v, %v; want boom", err, ok)
			}
			if err := h.Stop(t.Context()); err != nil {
				t.Fatalf("Stop() = %v", err)
			}
		})
	})
	t.Run("nil before stop goes to Err", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := jobqueue.NewWorker(runFunc(func(context.Context, map[string]endpoint.Endpoint) error {
				return nil
			}), nil)
			if err := h.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err, ok := <-h.(errReporter).Err(); !ok || err == nil {
				t.Fatalf("Err() = %v, %v; want an unexpected-end error", err, ok)
			}
		})
	})
	tests := []struct {
		name string
		ret  error
		want error
	}{
		{"canceled while stopping is clean", context.Canceled, nil},
		{"nil while stopping is clean", nil, nil},
		{"other error while stopping is returned", errBoom, errBoom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := jobqueue.NewWorker(blockUntilDone(tt.ret), nil)
				if err := h.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				err := h.Stop(t.Context())
				if (tt.want == nil && err != nil) || (tt.want != nil && !errors.Is(err, tt.want)) {
					t.Fatalf("Stop() = %v, want %v", err, tt.want)
				}
			})
		})
	}
}

func TestWorkerMetadataAndMiddleware(t *testing.T) {
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
					if info, ok := metadata.FromContext(ctx); !ok || info.Protocol != "jobqueue" {
						t.Errorf("middleware %s: Info missing: %+v", name, info)
					}
					record(name + ">")
					defer record("<" + name)
					return next(ctx, req)
				}
			}
		}
		var gotInfo metadata.Info
		handlers := map[string]endpoint.Endpoint{
			"email": func(ctx context.Context, req any) (any, error) {
				gotInfo, _ = metadata.FromContext(ctx)
				record("email")
				return nil, errBoom
			},
		}
		results := make(chan error, 1)
		r := runFunc(func(ctx context.Context, hs map[string]endpoint.Endpoint) error {
			job := &jobqueue.Job{ID: "1", Type: "email", Header: metadata.Pairs("tenant", "acme"), Attempt: 1}
			_, err := hs["email"](ctx, job)
			results <- err
			<-ctx.Done()
			return nil
		})
		h := jobqueue.NewWorker(r, handlers, jobqueue.WithMiddleware(mw("a")), jobqueue.WithMiddleware(mw("b")))
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := <-results; !errors.Is(err, errBoom) {
			t.Fatalf("handler error = %v, want boom", err)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if gotInfo.Kind != metadata.KindServer || gotInfo.Protocol != "jobqueue" ||
			gotInfo.Operation != "email" || gotInfo.Header.Get("tenant") != "acme" {
			t.Fatalf("Info = %+v", gotInfo)
		}
		if want := []string{"a>", "b>", "email", "<b", "<a"}; !slices.Equal(trace, want) {
			t.Fatalf("call order = %v, want %v", trace, want)
		}
	})
}

func TestWorkerFailureStopsApp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := runFunc(func(ctx context.Context, _ map[string]endpoint.Endpoint) error {
			select {
			case <-time.After(time.Second):
				return errBoom
			case <-ctx.Done():
				return nil
			}
		})
		a := app.New(
			app.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
			app.WithHooks(jobqueue.NewWorker(r, nil)),
		)
		if err := a.Run(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("Run() = %v, want boom", err)
		}
		if got := a.State(); got != app.StateFailed {
			t.Fatalf("State() = %v, want %v", got, app.StateFailed)
		}
	})
}
