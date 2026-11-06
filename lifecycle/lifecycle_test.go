package lifecycle

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/app"
)

func TestAssemblyFailureAndOwnership(t *testing.T) {
	want := errors.New("construction failed")
	var closed atomic.Int32
	assemble := func() (err error) {
		scope, _ := NewScope(time.Second)
		defer scope.Finish(&err)
		resource, _ := New("db", 42, Operations[int]{Close: func(context.Context, int) error { closed.Add(1); return nil }})
		if err := scope.Add(resource); err != nil {
			return err
		}
		return want
	}
	if err := assemble(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatal("resource leaked before Start")
	}
	borrowed, _ := New("borrowed", 42, Operations[int]{})
	if err := borrowed.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedStartupReleasesAllResources(t *testing.T) {
	wantPanic := errors.New("startup panic")
	for _, tt := range []struct {
		name      string
		abort     func()
		wantPanic any
	}{
		{name: "panic", abort: func() { panic(wantPanic) }, wantPanic: wantPanic},
		{name: "goexit", abort: runtime.Goexit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				scope, err := NewScope(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				var closed []string
				for _, name := range []string{"one", "two", "three"} {
					resource, err := New(name, name, Operations[string]{
						Check: func(context.Context, string) error {
							if name == "two" {
								tt.abort()
							}
							return nil
						},
						Close: func(_ context.Context, value string) error {
							closed = append(closed, value)
							return nil
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := scope.Add(resource); err != nil {
						t.Fatal(err)
					}
				}
				var recovered any
				returned := false
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { recovered = recover() }()
					_ = scope.Start(t.Context())
					returned = true
				}()
				<-done
				if returned || recovered != tt.wantPanic {
					t.Fatalf("startup interruption changed: returned=%v panic=%v", returned, recovered)
				}
				if !slices.Equal(closed, []string{"three", "two", "one"}) {
					t.Fatalf("constructed resources not released in reverse: %v", closed)
				}
				if err := scope.Check(t.Context()); !errors.Is(err, ErrNotReady) {
					t.Fatalf("interrupted scope reported ready: %v", err)
				}
				if err := scope.Stop(t.Context()); err != nil {
					t.Fatalf("repeated cleanup failed: %v", err)
				}
			})
		})
	}
}

func TestEmptyScopeReadinessAndTypedNil(t *testing.T) {
	scope, _ := NewScope(time.Second)
	if err := scope.Check(t.Context()); !errors.Is(err, ErrNotReady) {
		t.Fatal("premature readiness", err)
	}
	var resource *Resource[int]
	if err := scope.Add(resource); err == nil {
		t.Fatal("typed nil resource accepted")
	}
	if err := scope.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := scope.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := scope.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := scope.Check(t.Context()); !errors.Is(err, ErrNotReady) {
		t.Fatal("stopped empty scope ready", err)
	}
}

func TestCanceledStartupReleasesCreatedResourcesAndFinish(t *testing.T) {
	var closed atomic.Int32
	want := errors.New("startup canceled")
	run := func() (err error) {
		scope, _ := NewScope(time.Second)
		defer scope.Finish(&err)
		for _, name := range []string{"one", "two"} {
			resource, _ := New(name, 0, Operations[int]{Close: func(context.Context, int) error { closed.Add(1); return nil }})
			if err := scope.Add(resource); err != nil {
				return err
			}
		}
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(want)
		return scope.Start(ctx)
	}
	if err := run(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if closed.Load() != 2 {
		t.Fatal("created resources were not released", closed.Load())
	}
}

func TestStopWaitsForStartupBeforeClosing(t *testing.T) {
	ctx, cancelOperation := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelOperation()
	scope, _ := NewScope(time.Second)
	entered := make(chan struct{})
	finishStart := make(chan struct{})
	releaseStart := sync.OnceFunc(func() { close(finishStart) })
	defer releaseStart()
	closed := make(chan string, 2)
	var secondStarts atomic.Int32
	first := app.NewHook("one", func(context.Context) error { close(entered); <-finishStart; return nil }, func(context.Context) error { closed <- "one"; return nil })
	second := app.NewHook("two", func(context.Context) error { secondStarts.Add(1); return nil }, func(context.Context) error { closed <- "two"; return nil })
	if err := scope.Add(first); err != nil {
		t.Fatal(err)
	}
	if err := scope.Add(second); err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	startupDone := make(chan struct{})
	go func() {
		defer close(startupDone)
		startResult <- scope.Start(ctx)
	}()
	t.Cleanup(func() { releaseStart(); awaitLifecycle(t, startupDone) })
	awaitLifecycle(t, entered)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := scope.Stop(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case name := <-closed:
		t.Fatal("closed during startup", name)
	default:
	}
	releaseStart()
	if err := awaitLifecycle(t, startResult); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := scope.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if secondStarts.Load() != 0 {
		t.Fatal("started another resource after stop requested")
	}
	if got := awaitLifecycle(t, closed); got != "two" {
		t.Fatal(got)
	}
	if got := awaitLifecycle(t, closed); got != "one" {
		t.Fatal(got)
	}
}

func TestStopDeadlineAndLaterJoinPreserveDependencyOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scope, _ := NewScope(100 * time.Millisecond)
		closing := make(chan struct{})
		release := make(chan struct{})
		order := make(chan string, 2)
		var slowCalls atomic.Int32
		dependency, _ := New("dependency", 0, Operations[int]{Close: func(context.Context, int) error { order <- "dependency"; return nil }})
		consumer, _ := New("consumer", 0, Operations[int]{Close: func(context.Context, int) error {
			slowCalls.Add(1)
			close(closing)
			<-release
			order <- "consumer"
			return nil
		}})
		if err := scope.Add(dependency); err != nil {
			t.Fatal(err)
		}
		if err := scope.Add(consumer); err != nil {
			t.Fatal(err)
		}
		if err := scope.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		short, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := scope.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		<-closing
		select {
		case got := <-order:
			t.Fatal("closed dependency before consumer completed", got)
		default:
		}
		close(release)
		synctest.Wait()
		// Even a canceled repeat call returns the completed cleanup result.
		canceled, stop := context.WithCancel(t.Context())
		stop()
		if err := scope.Stop(canceled); err != nil {
			t.Fatal("false repeated cancellation", err)
		}
		if err := consumer.Stop(canceled); err != nil {
			t.Fatal("false resource cancellation", err)
		}
		if got := <-order; got != "consumer" {
			t.Fatal(got)
		}
		if got := <-order; got != "dependency" {
			t.Fatal(got)
		}
		if slowCalls.Load() != 1 {
			t.Fatal("repeated cleanup", slowCalls.Load())
		}
	})
}

func TestCheckAdmittedBeforeStopCanOverlapButNeverReturnReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	scope, _ := NewScope(time.Second)
	checking := make(chan struct{})
	finishCheck := make(chan struct{})
	releaseCheck := sync.OnceFunc(func() { close(finishCheck) })
	defer releaseCheck()
	var probes atomic.Int32
	resource, _ := New("db", 0, Operations[int]{Check: func(context.Context, int) error {
		if probes.Add(1) > 1 {
			close(checking)
			<-finishCheck
		}
		return nil
	}, Close: func(context.Context, int) error { return nil }})
	if err := scope.Add(resource); err != nil {
		t.Fatal(err)
	}
	if err := scope.Start(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	checkDone := make(chan struct{})
	go func() {
		defer close(checkDone)
		result <- scope.Check(ctx)
	}()
	t.Cleanup(func() { releaseCheck(); awaitLifecycle(t, checkDone) })
	awaitLifecycle(t, checking)
	if err := scope.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	releaseCheck()
	if err := awaitLifecycle(t, result); !errors.Is(err, ErrStopped) && !errors.Is(err, ErrNotReady) {
		t.Fatal("reported ready after close", err)
	}
}

func awaitLifecycle[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle handshake did not finish")
		var zero T
		return zero
	}
}

func TestStartFailureCleansAllInReverse(t *testing.T) {
	scope, _ := NewScope(time.Second)
	order := make(chan string, 3)
	for _, name := range []string{"one", "two", "three"} {
		resource, _ := New(name, name, Operations[string]{Check: func(_ context.Context, name string) error {
			if name == "two" {
				return errors.New("unready")
			}
			return nil
		}, Close: func(_ context.Context, name string) error { order <- name; return nil }})
		if err := scope.Add(resource); err != nil {
			t.Fatal(err)
		}
	}
	if err := scope.Start(t.Context()); err == nil {
		t.Fatal("missing startup failure")
	}
	if err := scope.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := scope.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"three", "two", "one"} {
		if got := <-order; got != want {
			t.Fatal(got, want)
		}
	}
}
