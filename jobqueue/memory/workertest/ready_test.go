package jobqueue_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/jobqueue/memory"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/jobqueue"
)

// readyRunner is a ReadyRunner driven by a function.
type readyRunner struct {
	runFunc
	ready func(ctx context.Context, ready func()) error
}

func (r readyRunner) RunReady(ctx context.Context, _ map[string]endpoint.Endpoint, ready func()) error {
	return r.ready(ctx, ready)
}

type checker interface{ Check(context.Context) error }

func TestWorkerWithName(t *testing.T) {
	h := jobqueue.NewWorker(blockUntilDone(nil), nil, jobqueue.WithName("mail-worker"))
	if got := h.Name(); got != "mail-worker" {
		t.Errorf("Name() = %q, want mail-worker", got)
	}
}

func TestWorkerStartWaitsForReadyRunner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		h := jobqueue.NewWorker(readyRunner{ready: func(ctx context.Context, ready func()) error {
			<-release
			ready()
			<-ctx.Done()
			return nil
		}}, nil)
		started := make(chan error, 1)
		go func() { started <- h.Start(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-started:
			t.Fatalf("Start returned %v before the runner was ready", err)
		default:
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

func TestWorkerReadyFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := jobqueue.NewWorker(readyRunner{ready: func(context.Context, func()) error { return errBoom }}, nil)
		if err := h.Start(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("Start() = %v, want %v", err, errBoom)
		}
		if _, ok := <-h.(errReporter).Err(); ok {
			t.Error("start failure also reported through Err")
		}

		h = jobqueue.NewWorker(readyRunner{ready: func(ctx context.Context, _ func()) error {
			<-ctx.Done()
			return nil
		}}, nil)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := h.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start() = %v, want DeadlineExceeded", err)
		}
	})
}

// A second worker on the same memory queue fails to start instead of
// reporting the conflict after the App is already running.
func TestWorkerMemoryQueueConflictFailsStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := memory.New()
		defer q.Close()
		first := jobqueue.NewWorker(q, nil)
		if err := first.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Stop(t.Context()) }()
		second := jobqueue.NewWorker(q, nil, jobqueue.WithName("second"))
		if err := second.Start(t.Context()); err == nil {
			_ = second.Stop(t.Context())
			t.Fatal("a second worker on the same queue started")
		}
	})
}

// A job enqueued right after Start returns is processed.
func TestWorkerProcessesJobEnqueuedAfterStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := memory.New()
		defer q.Close()
		done := make(chan string, 1)
		h := jobqueue.NewWorker(q, map[string]endpoint.Endpoint{"mail": func(_ context.Context, req any) (any, error) {
			done <- req.(*jobqueue.Job).ID
			return nil, nil
		}})
		if err := h.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := h.(checker).Check(t.Context()); err != nil {
			t.Errorf("Check() = %v", err)
		}
		id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "mail"})
		if err != nil {
			t.Fatal(err)
		}
		if got := <-done; got != id {
			t.Errorf("processed %q, want %q", got, id)
		}
		if err := h.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}
