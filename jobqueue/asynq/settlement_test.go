package asynq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/jobqueue"
)

// A processor may settle an attempt while its handler is still running. Wait
// for both independently so the regression cannot pass just because the late
// handler has not reached its reservation code yet.
func (b backend) testLateHandlerCannotReleaseReservation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		timeout  bool
		shutdown bool
		complete bool
		retries  int
		lateErr  error
		state    asynq.TaskState
	}{
		{name: "canceled success", retries: 1, state: asynq.TaskStateRetry},
		{name: "canceled rejection", retries: 1, lateErr: errors.New("late rejection"), state: asynq.TaskStateRetry},
		{name: "timeout", timeout: true, retries: 1, state: asynq.TaskStateRetry},
		{name: "exhausted", state: asynq.TaskStateArchived},
		{name: "shutdown", shutdown: true, retries: 1, state: asynq.TaskStatePending},
		{name: "retained completion", complete: true, state: asynq.TaskStateCompleted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := b.newQueue(t)
			key, id := q.uniqueKey("guard"), "original"
			if ok, err := q.reserve(t.Context(), key, id, reserveGrace); err != nil || !ok {
				t.Fatalf("reserve = %v, %v", ok, err)
			}
			headers, err := encodeHeaders(nil, key)
			if err != nil {
				t.Fatal(err)
			}
			opts := []asynq.Option{asynq.TaskID(id), asynq.Queue(q.opts.queue), asynq.MaxRetry(tt.retries)}
			if tt.timeout {
				opts = append(opts, asynq.Timeout(time.Second))
			}
			if tt.complete {
				opts = append(opts, asynq.Retention(time.Hour))
			}
			if _, err := q.client.EnqueueContext(t.Context(), asynq.NewTaskWithHeaders("work", nil, headers), opts...); err != nil {
				t.Fatal(err)
			}
			q.confirm(t.Context(), key, id)

			entered, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(finish) }) }
			h := q.handler(t.Context(), map[string]endpoint.Endpoint{"work": func(ctx context.Context, _ any) (any, error) {
				close(entered)
				if !tt.complete {
					<-ctx.Done()
				}
				<-finish
				return nil, tt.lateErr
			}})
			srv := asynq.NewServerFromRedisClient(q.rdb, asynq.Config{
				Concurrency: 1, Queues: map[string]int{q.opts.queue: 1},
				TaskCheckInterval: 10 * time.Millisecond, ShutdownTimeout: 20 * time.Millisecond,
				RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return time.Hour },
				Logger:         logger{q.opts.logger}, LogLevel: asynq.WarnLevel,
			})
			if err := srv.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
				err := h(ctx, task)
				close(returned)
				return err
			})); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(srv.Shutdown)
			t.Cleanup(release)
			select {
			case <-entered:
			case <-time.After(waitTimeout):
				t.Fatal("handler did not start")
			}
			switch {
			case tt.complete:
				release()
			case tt.shutdown:
				srv.Shutdown()
			case !tt.timeout:
				if err := q.inspector.CancelProcessing(id); err != nil {
					t.Fatal(err)
				}
			}
			eventually(t, "processor settlement", func() bool {
				info, err := q.inspector.GetTaskInfo(q.opts.queue, id)
				return err == nil && info.State == tt.state
			})
			release()
			select {
			case <-returned:
			case <-time.After(waitTimeout):
				t.Fatal("late handler did not return")
			}
			if got := q.rdb.Get(t.Context(), key).Val(); got != id {
				t.Fatalf("late handler changed reservation to %q", got)
			}
			_, err = q.Enqueue(t.Context(), &jobqueue.Job{Type: "other"}, jobqueue.WithUnique("guard", 0), jobqueue.WithDelay(time.Hour))
			if tt.state == asynq.TaskStateArchived || tt.state == asynq.TaskStateCompleted {
				if err != nil {
					t.Fatalf("terminal reservation not reclaimed: %v", err)
				}
			} else if !errors.Is(err, jobqueue.ErrDuplicate) {
				t.Fatalf("live task lost uniqueness: %v", err)
			}
		})
	}
}
