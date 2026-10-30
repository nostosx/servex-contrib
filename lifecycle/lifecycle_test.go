package lifecycle

import (
	"context"
	"errors"
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
	scope, _ := NewScope(time.Second)
	entered := make(chan struct{})
	finishStart := make(chan struct{})
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
	go func() { startResult <- scope.Start(t.Context()) }()
	<-entered
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
	close(finishStart)
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := scope.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if secondStarts.Load() != 0 {
		t.Fatal("started another resource after stop requested")
	}
	if got := <-closed; got != "two" {
		t.Fatal(got)
	}
	if got := <-closed; got != "one" {
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
	scope, _ := NewScope(time.Second)
	checking := make(chan struct{})
	finishCheck := make(chan struct{})
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
	if err := scope.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- scope.Check(t.Context()) }()
	<-checking
	if err := scope.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(finishCheck)
	if err := <-result; !errors.Is(err, ErrStopped) && !errors.Is(err, ErrNotReady) {
		t.Fatal("reported ready after close", err)
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
