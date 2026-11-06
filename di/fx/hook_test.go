package fx_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uberfx "go.uber.org/fx"

	fxbridge "github.com/nostosx/servex-contrib/di/fx"
	"github.com/nostosx/servex/app"
)

type drainingHook struct {
	app.Hook
	drain func(context.Context) error
}

func (h drainingHook) Drain(ctx context.Context) error { return h.drain(ctx) }

func TestNativeLifecycleAndReadiness(t *testing.T) {
	var events []string
	native := uberfx.New(uberfx.NopLogger, uberfx.Invoke(func(lc uberfx.Lifecycle) {
		events = append(events, "constructed")
		lc.Append(uberfx.Hook{
			OnStart: func(context.Context) error { events = append(events, "fx.start"); return nil },
			OnStop:  func(context.Context) error { events = append(events, "fx.stop"); return nil },
		})
	}))
	bridge := fxbridge.Hook("dependencies", native)
	if bridge.Name() != "dependencies" || bridge.Check(t.Context()) == nil {
		t.Fatal("bridge must not report ready before startup")
	}
	server := drainingHook{
		Hook: app.NewHook("server", func(context.Context) error { events = append(events, "server.start"); return nil },
			func(context.Context) error { events = append(events, "server.stop"); return nil }),
		drain: func(context.Context) error { events = append(events, "server.drain"); return nil },
	}
	application := app.New(app.WithHooks(bridge, server))
	if err := application.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := application.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := application.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := bridge.Stop(t.Context()); err != nil || bridge.Check(t.Context()) == nil {
		t.Fatal("repeat cleanup or stopped readiness incorrect", err)
	}
	want := []string{"constructed", "fx.start", "server.start", "server.drain", "server.stop", "fx.stop"}
	if !slices.Equal(events, want) {
		t.Fatalf("lifecycle order = %v", events)
	}
	if err := bridge.Start(t.Context()); err == nil {
		t.Fatal("stopped bridge restarted")
	}
}

func TestInitializationAndStartupFailures(t *testing.T) {
	want := errors.New("initialization failed")
	bridge := fxbridge.Hook("dependencies", uberfx.New(uberfx.NopLogger, uberfx.Error(want)))
	if err := bridge.Start(t.Context()); !errors.Is(err, want) {
		t.Fatalf("initialization error lost: %v", err)
	}
	if err := bridge.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Int32
	want = errors.New("startup failed")
	native := uberfx.New(uberfx.NopLogger, uberfx.Invoke(func(lc uberfx.Lifecycle) {
		lc.Append(uberfx.Hook{OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { stopped.Add(1); return nil }})
		lc.Append(uberfx.Hook{OnStart: func(context.Context) error { return want }})
	}))
	bridge = fxbridge.Hook("dependencies", native)
	if err := bridge.Start(t.Context()); !errors.Is(err, want) {
		t.Fatalf("startup error lost: %v", err)
	}
	if stopped.Load() != 1 || bridge.Check(t.Context()) == nil {
		t.Fatal("failed startup was not rolled back exactly once")
	}
	if err := bridge.Stop(t.Context()); err != nil || stopped.Load() != 1 {
		t.Fatal("rollback duplicated on repeat Stop", err)
	}
}

func TestStopCancelsStartupAndJoinsCleanup(t *testing.T) {
	started := make(chan struct{})
	native := uberfx.New(uberfx.NopLogger, uberfx.Invoke(func(lc uberfx.Lifecycle) {
		lc.Append(uberfx.Hook{OnStart: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}})
	}))
	bridge := fxbridge.Hook("dependencies", native)
	result := make(chan error, 1)
	go func() { result <- bridge.Start(t.Context()) }()
	<-started
	if err := bridge.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("startup was not canceled: %v", err)
	}
}

func TestStopWaitersHaveIndependentContexts(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	releaseCleanup := sync.OnceFunc(func() { close(release) })
	defer releaseCleanup()
	var closed atomic.Int32
	native := uberfx.New(uberfx.NopLogger, uberfx.Invoke(func(lc uberfx.Lifecycle) {
		lc.Append(uberfx.Hook{OnStop: func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			close(started)
			<-release
			closed.Add(1)
			return nil
		}})
	}))
	bridge := fxbridge.Hook("dependencies", native)
	if err := bridge.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := bridge.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller kept waiting: %v", err)
	}
	<-started
	releaseCleanup()
	if err := bridge.Stop(t.Context()); err != nil || closed.Load() != 1 {
		t.Fatalf("shared cleanup = %d, error=%v", closed.Load(), err)
	}
}

func TestConfiguredStartupDeadline(t *testing.T) {
	native := uberfx.New(uberfx.NopLogger, uberfx.StartTimeout(10*time.Millisecond), uberfx.Invoke(func(lc uberfx.Lifecycle) {
		lc.Append(uberfx.Hook{OnStart: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }})
	}))
	bridge := fxbridge.Hook("dependencies", native)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := bridge.Start(ctx); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("native startup budget not applied: %v, parent=%v", err, ctx.Err())
	}
}

func TestNativeShutdownRequests(t *testing.T) {
	for _, code := range []int{0, 17} {
		t.Run(fmt.Sprintf("exit_%d", code), func(t *testing.T) {
			var shutdowner uberfx.Shutdowner
			var stopped atomic.Int32
			native := uberfx.New(uberfx.NopLogger, uberfx.Populate(&shutdowner), uberfx.Invoke(func(lc uberfx.Lifecycle) {
				lc.Append(uberfx.Hook{OnStop: func(context.Context) error { stopped.Add(1); return nil }})
			}))
			bridge := fxbridge.Hook("dependencies", native)
			ready := make(chan struct{})
			application := app.New(app.WithHooks(bridge, app.NewHook("server", func(context.Context) error { close(ready); return nil }, nil)))
			result := make(chan error, 1)
			go func() { result <- bridge.Run(t.Context(), application) }()
			<-ready
			if err := shutdowner.Shutdown(uberfx.ExitCode(code)); err != nil {
				t.Fatal(err)
			}
			err := <-result
			if code == 0 {
				if err != nil || application.State() != app.StateStopped {
					t.Fatalf("normal shutdown = %v state=%s", err, application.State())
				}
			} else {
				var exit *fxbridge.ExitError
				if !errors.As(err, &exit) || exit.Code != code || application.State() != app.StateFailed {
					t.Fatalf("nonzero exit lost: %v state=%s", err, application.State())
				}
			}
			if stopped.Load() != 1 || bridge.Check(t.Context()) == nil {
				t.Fatal("shutdown did not finish native cleanup")
			}
			if err := bridge.Run(t.Context(), application); err == nil {
				t.Fatal("Run accepted an already used bridge")
			}
		})
	}
}

func TestRunCancellationAndRuntimeFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "context canceled", true: "runtime failure"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var stopped atomic.Int32
			native := uberfx.New(uberfx.NopLogger, uberfx.Invoke(func(lc uberfx.Lifecycle) {
				lc.Append(uberfx.Hook{OnStop: func(context.Context) error { stopped.Add(1); return nil }})
			}))
			bridge := fxbridge.Hook("dependencies", native)
			ready := make(chan struct{})
			failures := make(chan error, 1)
			hook := app.NewReadyRunHook("worker", "worker", func(ctx context.Context, started func()) error {
				started()
				select {
				case err := <-failures:
					return err
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			application := app.New(app.WithHooks(bridge, hook, app.NewHook("ready", func(context.Context) error { close(ready); return nil }, nil)))
			result := make(chan error, 1)
			go func() { result <- bridge.Run(ctx, application) }()
			<-ready
			want := errors.New("worker failed")
			if fail {
				failures <- want
			} else {
				cancel()
			}
			err := <-result
			if fail && !errors.Is(err, want) || !fail && err != nil || stopped.Load() != 1 {
				t.Fatalf("Run result=%v cleanup=%d", err, stopped.Load())
			}
		})
	}
}

func TestQueuedNonzeroShutdownSurvivesParentCancellation(t *testing.T) {
	for range 12 {
		ctx, cancel := context.WithCancel(t.Context())
		var shutdowner uberfx.Shutdowner
		native := uberfx.New(uberfx.NopLogger, uberfx.Populate(&shutdowner))
		bridge := fxbridge.Hook("dependencies", native)
		ready := make(chan struct{})
		application := app.New(app.WithHooks(bridge,
			app.NewHook("ready", func(context.Context) error { close(ready); return nil }, nil)))
		result := make(chan error, 1)
		go func() { result <- bridge.Run(ctx, application) }()
		<-ready
		if err := shutdowner.Shutdown(uberfx.ExitCode(23)); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		var exit *fxbridge.ExitError
		if err := <-result; !errors.As(err, &exit) || exit.Code != 23 {
			t.Fatalf("queued native exit lost to cancellation: %v", err)
		}
	}
}

func TestRunStartupFailureStopsNativeReceiver(t *testing.T) {
	want := errors.New("before fx")
	bridge := fxbridge.Hook("dependencies", uberfx.New(uberfx.NopLogger))
	application := app.New(app.WithHooks(app.NewHook("failure", func(context.Context) error { return want }, nil), bridge))
	if err := bridge.Run(t.Context(), application); !errors.Is(err, want) {
		t.Fatal(err)
	}
	select {
	case _, open := <-bridge.Err():
		if open {
			t.Fatal("bridge error channel not closed")
		}
	default:
		t.Fatal("receiver cleanup unfinished")
	}
}

func TestRunInterruptedStartupReleasesNativeReceiver(t *testing.T) {
	marker := errors.New("startup panic")
	for _, tc := range []struct {
		name string
		stop func()
		want any
	}{
		{name: "panic", stop: func() { panic(marker) }, want: marker},
		{name: "Goexit", stop: runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := fxbridge.Hook("dependencies", uberfx.New(uberfx.NopLogger))
			application := app.New(app.WithHooks(app.NewHook("interrupted", func(context.Context) error {
				tc.stop()
				return nil
			}, nil), bridge))
			result := make(chan any, 1)
			go func() {
				defer func() { result <- recover() }()
				_ = bridge.Run(t.Context(), application)
			}()
			if got := <-result; got != tc.want {
				t.Fatalf("interrupted startup = %v, want %v", got, tc.want)
			}
			select {
			case _, open := <-bridge.Err():
				if open {
					t.Fatal("bridge error channel not closed")
				}
			default:
				t.Fatal("receiver cleanup unfinished after interrupted startup")
			}
			if err := bridge.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidAssembly(t *testing.T) {
	for _, fn := range []func(){func() { fxbridge.Hook("", uberfx.New(uberfx.NopLogger)) }, func() { fxbridge.Hook("dependencies", nil) }} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid assembly accepted")
				}
			}()
			fn()
		}()
	}
}
