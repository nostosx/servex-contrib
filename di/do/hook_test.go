package do_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/do/v2"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/health"
)

func newInjector(t *testing.T) *do.RootScope {
	t.Helper()
	i := do.New()
	t.Cleanup(func() { _ = dox.Shutdown(context.Background(), i) })
	return i
}

func TestHook(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	do.Provide(i, func(do.Injector) (string, error) {
		t.Error("lazy provider must not run during lifecycle operations")
		return "unused", nil
	})
	h := dox.Hook("dependencies", i)
	if h.Name() != "dependencies" {
		t.Fatalf("Name = %q", h.Name())
	}
	if err := h.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := h.(health.Checker).Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := h.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHookRejectsNil(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("Hook(nil) did not panic")
		}
	}()
	dox.Hook("dependencies", nil)
}

func TestHookStartCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := dox.Hook("dependencies", newInjector(t)).Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v", err)
	}
}

type resource struct {
	check    func(context.Context) error
	shutdown func(context.Context) error
}

func (r *resource) HealthCheck(ctx context.Context) error {
	if r.check != nil {
		return r.check(ctx)
	}
	return nil
}

func (r *resource) Shutdown(ctx context.Context) error {
	if r.shutdown != nil {
		return r.shutdown(ctx)
	}
	return nil
}

func TestHookCheck(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	first, second := errors.New("first failure"), errors.New("second failure")
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "check")
	for name, failure := range map[string]error{"first": first, "second": second} {
		do.ProvideNamedValue(i, name, &resource{check: func(ctx context.Context) error {
			if ctx.Value(contextKey{}) != "check" {
				t.Error("health check lost context value")
			}
			return failure
		}})
	}
	err := dox.Hook("dependencies", i).(health.Checker).Check(ctx)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("Check lost causes: %v", err)
	}
	if !strings.Contains(err.Error(), `health check "first"`) || !strings.Contains(err.Error(), `health check "second"`) {
		t.Fatalf("Check lost service names: %v", err)
	}
}

func TestHookCheckCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := dox.Hook("dependencies", newInjector(t)).(health.Checker).Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check = %v", err)
	}
}

func TestHookCheckCancellationInFlight(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	entered := make(chan struct{})
	do.ProvideValue(i, &resource{check: func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- dox.Hook("dependencies", i).(health.Checker).Check(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Check lost cancellation: %v", err)
	}
}

func TestHookStopIdempotent(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	failure := errors.New("shutdown failed")
	var calls atomic.Int32
	do.ProvideValue(i, &resource{shutdown: func(context.Context) error {
		calls.Add(1)
		return failure
	}})
	h := dox.Hook("dependencies", i)
	if err := h.Stop(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("Stop = %v", err)
	}
	if err := h.Stop(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("repeated Stop lost failure: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("shutdown called %d times", calls.Load())
	}
	if err := h.(health.Checker).Check(t.Context()); err == nil {
		t.Fatal("stopped hook reported ready")
	}
	if err := h.Start(t.Context()); err == nil {
		t.Fatal("stopped hook restarted")
	}
}

func TestHookConcurrentCheckStop(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	checking := make(chan struct{})
	release := make(chan struct{})
	var active atomic.Bool
	do.ProvideValue(i, &resource{
		check: func(context.Context) error {
			active.Store(true)
			close(checking)
			<-release
			active.Store(false)
			return nil
		},
		shutdown: func(context.Context) error {
			if active.Load() {
				t.Error("shutdown overlapped health check")
			}
			return nil
		},
	})
	h := dox.Hook("dependencies", i)
	checked := make(chan error, 1)
	go func() { checked <- h.(health.Checker).Check(t.Context()) }()
	<-checking
	stopped := make(chan error, 1)
	go func() { stopped <- h.Stop(t.Context()) }()
	close(release)
	if err := <-checked; err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

func TestHookAppLifecycle(t *testing.T) {
	t.Parallel()
	for _, failStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal stop", true: "start rollback"}[failStart], func(t *testing.T) {
			i := newInjector(t)
			var mu sync.Mutex
			var stopped []string
			record := func(name string) func(context.Context) error {
				return func(context.Context) error {
					mu.Lock()
					defer mu.Unlock()
					stopped = append(stopped, name)
					return nil
				}
			}
			do.ProvideNamed(i, "database", func(do.Injector) (*resource, error) {
				return &resource{shutdown: record("database")}, nil
			})
			do.ProvideNamed(i, "repository", func(i do.Injector) (*resource, error) {
				do.MustInvokeNamed[*resource](i, "database")
				return &resource{shutdown: record("repository")}, nil
			})
			if _, err := do.InvokeNamed[*resource](i, "repository"); err != nil {
				t.Fatal(err)
			}
			// A child scope is closed before its parent dependencies.
			do.ProvideValue(i.Scope("request"), &resource{shutdown: record("child")})
			failure := errors.New("listen failed")
			hooks := []app.Hook{
				dox.Hook("dependencies", i),
				app.NewHook("server", nil, record("server")),
			}
			if failStart {
				hooks = append(hooks, app.NewHook("failing", func(context.Context) error { return failure }, nil))
			}
			a := app.New(app.WithHooks(hooks...), app.WithLogger(testLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))))
			err := a.Start(t.Context())
			if failStart {
				if !errors.Is(err, failure) {
					t.Fatalf("Start = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Check(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := a.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if want := []string{"server", "child", "repository", "database"}; !slices.Equal(stopped, want) {
				t.Fatalf("stop order = %v, want %v", stopped, want)
			}
		})
	}
}

func TestShutdownErrors(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	first, second := errors.New("close database"), errors.New("close cache")
	do.ProvideNamedValue(i, "database", &resource{shutdown: func(context.Context) error { return first }})
	do.ProvideNamedValue(i.Scope("child"), "cache", &resource{shutdown: func(context.Context) error { return second }})
	err := dox.Shutdown(t.Context(), i)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("Shutdown lost causes: %v", err)
	}
	if !strings.Contains(err.Error(), `"database"`) || !strings.Contains(err.Error(), `"cache" in scope "child"`) {
		t.Fatalf("Shutdown lost service/scope names: %v", err)
	}
}

func TestShutdownContext(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "stop")
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var called atomic.Bool
	do.ProvideValue(i, &resource{shutdown: func(ctx context.Context) error {
		called.Store(true)
		if ctx.Value(contextKey{}) != "stop" {
			t.Error("shutdown lost context value")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("shutdown lost deadline")
		}
		return nil
	}})
	if err := dox.Shutdown(ctx, i); err != nil {
		t.Fatal(err)
	}
	if !called.Load() {
		t.Fatal("shutdown was not called")
	}
}

func TestShutdownCanceled(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	do.ProvideValue(i, &resource{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := dox.Shutdown(ctx, i); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v", err)
	}
}

func TestShutdownAssemblyFailure(t *testing.T) {
	t.Parallel()
	i := newInjector(t)
	var closed atomic.Bool
	do.Provide(i, func(do.Injector) (*resource, error) {
		return &resource{shutdown: func(context.Context) error { closed.Store(true); return nil }}, nil
	})
	do.Provide(i, func(i do.Injector) (string, error) {
		do.MustInvoke[*resource](i)
		// The outer Invoke turns the missing-dependency panic into an error.
		do.MustInvoke[int](i)
		return "ready", nil
	})
	if _, err := do.Invoke[string](i); err == nil {
		t.Fatal("missing dependency did not fail assembly")
	}
	if err := dox.Shutdown(t.Context(), i); err != nil {
		t.Fatal(err)
	}
	if !closed.Load() {
		t.Fatal("constructed dependency leaked after assembly failed")
	}
}

func TestShutdownRejectsNil(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("Shutdown(nil) did not panic")
		}
	}()
	_ = dox.Shutdown(t.Context(), nil)
}
