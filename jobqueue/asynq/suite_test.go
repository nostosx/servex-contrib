package asynq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
)

// The tests in this file run against miniredis in queue_test.go and against
// a real Redis server in queue_integration_test.go.

const waitTimeout = 30 * time.Second

// backend provides access to a Redis server.
type backend struct {
	// client returns a new client connected to the server.
	client func(t *testing.T) redis.UniversalClient
	// advance lets d elapse for key expirations.
	advance func(t *testing.T, d time.Duration)
	// cleanup removes resources from an external backend after the worker stops.
	cleanup func(t *testing.T, q *Queue)
	// ttl is the unique key TTL used by the tests; advance must be able to
	// let it elapse.
	ttl time.Duration
}

// newQueue creates a Queue on its own asynq queue with short polling
// intervals.
func (b backend) newQueue(t *testing.T, opts ...Option) *Queue {
	t.Helper()
	rdb := b.client(t)
	base := []Option{
		WithQueue(fmt.Sprintf("test-%s", uuid.NewV7())),
		WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
		WithShutdownTimeout(300 * time.Millisecond),
		WithLogger(slog.New(slog.DiscardHandler)),
		WithServerConfig(func(cfg *asynq.Config) {
			cfg.TaskCheckInterval = 10 * time.Millisecond
			cfg.DelayedTaskCheckInterval = 20 * time.Millisecond
		}),
	}
	q := NewFromClient(rdb, append(base, opts...)...)
	t.Cleanup(func() {
		q.Close()
		if b.cleanup != nil {
			b.cleanup(t, q)
		}
		rdb.Close()
	})
	return q
}

// running is a Run call in the background.
type running struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returned
	err    error         // result of Run, valid once done is closed
}

func start(t *testing.T, q *Queue, handlers map[string]endpoint.Endpoint) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.err = q.Run(ctx, handlers)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(waitTimeout):
			t.Error("Run did not exit during cleanup")
		}
	})
	return r
}

func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
		return r.err
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after cancellation")
		return nil
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// jobLog records handled jobs.
type jobLog struct {
	mu   sync.Mutex
	jobs []jobqueue.Job
}

func (l *jobLog) add(job *jobqueue.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jobs = append(l.jobs, *job)
}

func (l *jobLog) all() []jobqueue.Job {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.jobs)
}

func (l *jobLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.jobs)
}

func (l *jobLog) attempts() []int {
	var out []int
	for _, j := range l.all() {
		out = append(out, j.Attempt)
	}
	return out
}

// failures records jobs reported through WithFailed.
type failures struct {
	mu   sync.Mutex
	jobs []*jobqueue.Job
	errs []error
}

func (f *failures) record(job *jobqueue.Job, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = append(f.jobs, job)
	f.errs = append(f.errs, err)
}

func (f *failures) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

func (b backend) testEnqueueAndProcess(t *testing.T) {
	q := b.newQueue(t)
	var log jobLog
	r := start(t, q, map[string]endpoint.Endpoint{
		"email": func(_ context.Context, req any) (any, error) {
			log.add(req.(*jobqueue.Job))
			return nil, nil
		},
	})
	single := metadata.Pairs("trace-id", "abc")
	multi := metadata.Join(metadata.Pairs("trace-id", "abc", "multi", "1"), metadata.Pairs("multi", "2"))
	id1, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "job-1", Type: "email", Payload: []byte("p1"), Header: single})
	if err != nil || id1 != "job-1" {
		t.Fatalf("Enqueue() = %q, %v", id1, err)
	}
	id2, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "email", Payload: []byte("p2"), Header: multi})
	if err != nil {
		t.Fatal(err)
	}
	if u, err := uuid.Parse(id2); err != nil || u[6]>>4 != 7 {
		t.Fatalf("generated ID %q is not a UUIDv7: %v", id2, err)
	}
	eventually(t, "two jobs", func() bool { return log.len() == 2 })
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	jobs := log.all()
	slices.SortFunc(jobs, func(a, b jobqueue.Job) int { return slices.Compare(a.Payload, b.Payload) })
	j1, j2 := jobs[0], jobs[1]
	if j1.ID != "job-1" || j1.Type != "email" || string(j1.Payload) != "p1" || j1.Attempt != 1 ||
		j1.Header.Get("trace-id") != "abc" || len(j1.Header) != 1 {
		t.Errorf("job 1 = %+v", j1)
	}
	if j2.ID != id2 || string(j2.Payload) != "p2" || j2.Attempt != 1 || j2.Header.Get("trace-id") != "abc" ||
		!slices.Equal(j2.Header.Values("multi"), []string{"1", "2"}) || len(j2.Header) != 2 {
		t.Errorf("job 2 = %+v", j2)
	}
}

func (b backend) testRetries(t *testing.T) {
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	tests := []struct {
		name         string
		opts         []jobqueue.EnqueueOption
		failures     int
		err          error
		wantAttempts []int
		wantFailed   bool
	}{
		{"success", nil, 0, nil, []int{1}, false},
		{"retryable then success", nil, 2, unavailable, []int{1, 2, 3}, false},
		{"retryable exhausts default retries", nil, 100, unavailable, []int{1, 2, 3, 4}, true},
		{"retryable exhausts custom retries", []jobqueue.EnqueueOption{jobqueue.MaxRetry(1)}, 100, unavailable, []int{1, 2}, true},
		{"zero retries", []jobqueue.EnqueueOption{jobqueue.MaxRetry(0)}, 100, unavailable, []int{1}, true},
		{"negative retries", []jobqueue.EnqueueOption{jobqueue.MaxRetry(-2)}, 100, unavailable, []int{1}, true},
		{"plain error is not retryable", nil, 100, errors.New("bad"), []int{1}, true},
		{"explicitly non-retryable", nil, 100, unavailable.WithRetryable(false), []int{1}, true},
		{"explicitly retryable", []jobqueue.EnqueueOption{jobqueue.MaxRetry(1)}, 100,
			errorx.New(errorx.Internal, "X", "x").WithRetryable(true), []int{1, 2}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				fails    failures
				mu       sync.Mutex
				backoffs []int
			)
			q := b.newQueue(t, WithFailed(fails.record), WithBackoff(func(attempt int) time.Duration {
				mu.Lock()
				defer mu.Unlock()
				backoffs = append(backoffs, attempt)
				return 10 * time.Millisecond
			}))
			var log jobLog
			r := start(t, q, map[string]endpoint.Endpoint{
				"work": func(_ context.Context, req any) (any, error) {
					job := req.(*jobqueue.Job)
					log.add(job)
					if job.Attempt <= tt.failures {
						return nil, tt.err
					}
					return nil, nil
				},
			})
			id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, "job settled", func() bool {
				if log.len() < len(tt.wantAttempts) {
					return false
				}
				return !tt.wantFailed || fails.len() == 1
			})
			time.Sleep(100 * time.Millisecond) // no further attempts
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			if got := log.attempts(); !slices.Equal(got, tt.wantAttempts) {
				t.Errorf("attempts = %v, want %v", got, tt.wantAttempts)
			}
			for _, j := range log.all() {
				if j.ID != id {
					t.Errorf("job ID = %q, want %q", j.ID, id)
				}
			}
			mu.Lock()
			if want := len(tt.wantAttempts) - 1; len(backoffs) != want {
				t.Errorf("backoff calls = %v, want %d", backoffs, want)
			}
			mu.Unlock()
			if got := fails.len(); got != map[bool]int{false: 0, true: 1}[tt.wantFailed] {
				t.Fatalf("failed jobs = %d, want failed=%v", got, tt.wantFailed)
			}
			if tt.wantFailed {
				if fails.jobs[0].ID != id || !errors.Is(fails.errs[0], tt.err) {
					t.Errorf("failed job = %+v, %v", fails.jobs[0], fails.errs[0])
				}
				insp := asynq.NewInspectorFromRedisClient(q.rdb)
				info, err := insp.GetTaskInfo(q.opts.queue, id)
				if err != nil || info.State != asynq.TaskStateArchived {
					t.Errorf("task info = %+v, %v; want archived", info, err)
				}
			}
		})
	}
}

func (b backend) testUnknownJobType(t *testing.T) {
	var fails failures
	q := b.newQueue(t, WithFailed(fails.record))
	start(t, q, map[string]endpoint.Endpoint{"known": func(context.Context, any) (any, error) { return nil, nil }})
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "unknown"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "failed job", func() bool { return fails.len() == 1 })
	if fails.jobs[0].Type != "unknown" || errorx.IsRetryable(fails.errs[0]) {
		t.Fatalf("failed = %+v, %v", fails.jobs[0], fails.errs[0])
	}
}

func (b backend) testDelay(t *testing.T) {
	q := b.newQueue(t)
	var log jobLog
	handled := make(chan time.Time, 2)
	start(t, q, map[string]endpoint.Endpoint{"work": func(_ context.Context, req any) (any, error) {
		log.add(req.(*jobqueue.Job))
		handled <- time.Now()
		return nil, nil
	}})
	begin := time.Now()
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Delay(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Delay(-time.Second)); err != nil {
		t.Fatal(err)
	}
	first := <-handled
	second := <-handled
	// asynq schedules with a precision of one second.
	if d := first.Sub(begin); d >= time.Second {
		t.Errorf("undelayed job ran after %v", d)
	}
	if d := second.Sub(begin); d < time.Second {
		t.Errorf("job delayed by 2s ran after %v, want at least 1s", d)
	}
}

func (b backend) testUnique(t *testing.T) {
	q := b.newQueue(t)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", b.ttl)); err != nil {
		t.Fatal(err)
	}
	_, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", b.ttl))
	if !errors.Is(err, jobqueue.ErrDuplicate) || errorx.CodeOf(err) != errorx.AlreadyExists {
		t.Fatalf("duplicate Enqueue() = %v, want ErrDuplicate", err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("other", b.ttl)); err != nil {
		t.Fatalf("Enqueue(other key) = %v", err)
	}
	b.advance(t, b.ttl+100*time.Millisecond)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", b.ttl)); err != nil {
		t.Fatalf("Enqueue() after the TTL = %v", err)
	}
}

func (b backend) testUniqueWithoutTTL(t *testing.T) {
	q := b.newQueue(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unlock := func() { releaseOnce.Do(func() { close(release) }) }
	// Release a blocked handler before start's cleanup waits for Run.
	defer unlock()
	var calls atomic.Int32
	start(t, q, map[string]endpoint.Endpoint{"work": func(ctx context.Context, req any) (any, error) {
		if calls.Add(1) == 1 {
			<-release
			return nil, nil
		}
		return nil, errors.New("bad") // fails for good: permits reservation reclamation
	}})
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "job running", func() bool { return calls.Load() == 1 })
	if ttl := q.rdb.TTL(t.Context(), q.uniqueKey("k")).Val(); ttl != -1 {
		t.Fatalf("TTL of the reservation = %v, want none", ttl)
	}
	b.advance(t, b.ttl+100*time.Millisecond)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatalf("Enqueue() while running = %v, want ErrDuplicate", err)
	}
	unlock()
	eventually(t, "reservation reclaimed after completion", func() bool {
		_, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0))
		return err == nil
	})
	eventually(t, "second job failed", func() bool { return calls.Load() == 2 })
	eventually(t, "reservation reclaimed after failure", func() bool {
		_, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0))
		return err == nil
	})
}

func (b backend) testUniqueReclaimedAfterPanic(t *testing.T) {
	var fails failures
	q := b.newQueue(t, WithFailed(fails.record))
	var calls atomic.Int32
	start(t, q, map[string]endpoint.Endpoint{"work": func(context.Context, any) (any, error) {
		calls.Add(1)
		panic("boom")
	}})
	id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0), jobqueue.MaxRetry(2))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "failed job", func() bool { return fails.len() == 1 })
	if calls.Load() != 1 {
		t.Errorf("handler calls = %d, want 1: a panic fails the job", calls.Load())
	}
	if fails.jobs[0].ID != id || errorx.CodeOf(fails.errs[0]) != errorx.Internal || errorx.IsRetryable(fails.errs[0]) {
		t.Errorf("failed job = %+v, %v; want non-retryable Internal", fails.jobs[0], fails.errs[0])
	}
	eventually(t, "panic archived by processor", func() bool {
		_, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "other"}, jobqueue.Unique("k", 0))
		return err == nil
	})
}

func (b backend) testUniqueReclaimedWhenTaskEnds(t *testing.T) {
	for _, tt := range []struct {
		name string
		end  func(insp *asynq.Inspector, queue, id string) error
	}{
		{"archived", func(insp *asynq.Inspector, queue, id string) error { return insp.ArchiveTask(queue, id) }},
		{"deleted", func(insp *asynq.Inspector, queue, id string) error { return insp.DeleteTask(queue, id) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := b.newQueue(t)
			insp := asynq.NewInspectorFromRedisClient(q.rdb)
			id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0), jobqueue.Delay(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			ttlID, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("ttl", time.Hour), jobqueue.Delay(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
				t.Fatalf("Enqueue() while scheduled = %v, want ErrDuplicate", err)
			}
			if err := tt.end(insp, q.opts.queue, id); err != nil {
				t.Fatal(err)
			}
			if err := tt.end(insp, q.opts.queue, ttlID); err != nil {
				t.Fatal(err)
			}
			// The job ended without a handler: Enqueue can reclaim its reservation.
			if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0)); err != nil {
				t.Fatalf("Enqueue() after the task was %s = %v", tt.name, err)
			}
			if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("k", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
				t.Fatalf("Enqueue() after taking over = %v, want ErrDuplicate", err)
			}
			// A positive TTL lasts for the TTL whatever happens to the job.
			if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("ttl", time.Hour)); !errors.Is(err, jobqueue.ErrDuplicate) {
				t.Fatalf("Enqueue(ttl) after the task was %s = %v, want ErrDuplicate", tt.name, err)
			}
		})
	}
}

func (b backend) testTaskIDConflict(t *testing.T) {
	q := b.newQueue(t)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "same", Type: "work"}); err != nil {
		t.Fatal(err)
	}
	_, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "same", Type: "work"}, jobqueue.Unique("u", 0))
	if errorx.CodeOf(err) != errorx.AlreadyExists || errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatalf("Enqueue(same ID) = %v, want AlreadyExists", err)
	}
	// The unique key reserved for the rejected job was released.
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.Unique("u", 0)); err != nil {
		t.Fatalf("Enqueue(u) = %v", err)
	}
}

func (b backend) testStopKeepsJob(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  func(ctx context.Context) error
	}{
		{"interrupted", func(ctx context.Context) error { return ctx.Err() }},
		{"retryable error while stopping", func(context.Context) error { return errorx.New(errorx.Unavailable, "DOWN", "down") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var fails failures
			q := b.newQueue(t, WithFailed(fails.record))
			started := make(chan struct{})
			stopping := make(chan struct{})
			var stoppingOnce sync.Once
			unblock := func() { stoppingOnce.Do(func() { close(stopping) }) }
			defer unblock()
			var once sync.Once
			r := start(t, q, map[string]endpoint.Endpoint{"work": func(ctx context.Context, _ any) (any, error) {
				once.Do(func() { close(started) })
				<-stopping // Run's context has ended
				if tt.name == "interrupted" {
					<-ctx.Done() // canceled at the end of the shutdown timeout
				}
				return nil, tt.err(ctx)
			}})
			if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "j", Type: "work"}); err != nil {
				t.Fatal(err)
			}
			<-started
			r.cancel()
			unblock()
			if err := r.stop(t); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			var log jobLog
			start(t, q, map[string]endpoint.Endpoint{"work": func(_ context.Context, req any) (any, error) {
				log.add(req.(*jobqueue.Job))
				return nil, nil
			}})
			eventually(t, "job picked up again", func() bool { return log.len() == 1 })
			if got := log.attempts(); !slices.Equal(got, []int{1}) {
				t.Fatalf("attempts after restart = %v, want [1]", got)
			}
			if fails.len() != 0 {
				t.Fatalf("job reported as failed")
			}
		})
	}
}

func (b backend) testNonRetryableWhileStoppingFails(t *testing.T) {
	var fails failures
	q := b.newQueue(t, WithFailed(fails.record))
	started := make(chan struct{})
	stopping := make(chan struct{})
	var stoppingOnce sync.Once
	unblock := func() { stoppingOnce.Do(func() { close(stopping) }) }
	defer unblock()
	r := start(t, q, map[string]endpoint.Endpoint{"work": func(context.Context, any) (any, error) {
		close(started)
		<-stopping
		return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad job")
	}})
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "j", Type: "work"}); err != nil {
		t.Fatal(err)
	}
	<-started
	r.cancel()
	unblock()
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if fails.len() != 1 {
		t.Fatalf("failed jobs = %d, want 1", fails.len())
	}
	info, err := asynq.NewInspectorFromRedisClient(q.rdb).GetTaskInfo(q.opts.queue, "j")
	if err != nil || info.State != asynq.TaskStateArchived {
		t.Fatalf("task info = %+v, %v; want archived", info, err)
	}
}

func (b backend) testEnqueueErrors(t *testing.T) {
	q := b.newQueue(t)
	for name, err := range map[string]error{
		"nil job":    func() error { _, err := q.Enqueue(t.Context(), nil); return err }(),
		"empty type": func() error { _, err := q.Enqueue(t.Context(), &jobqueue.Job{}); return err }(),
		"blank id":   func() error { _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work", ID: " "}); return err }(),
	} {
		if errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Errorf("%s: Enqueue() = %v, want InvalidArgument", name, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := q.Enqueue(ctx, &jobqueue.Job{Type: "work"}); errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("Enqueue(canceled) = %v, want Canceled", err)
	}
	if err := q.Run(ctx, nil); err != nil {
		t.Errorf("Run(canceled) = %v, want nil", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Errorf("Enqueue() after Close = %v, want FailedPrecondition", err)
	}
	if err := q.Run(t.Context(), nil); err != nil {
		t.Errorf("Run() after Close = %v, want nil", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("second Close() = %v", err)
	}
}

func (b backend) testCloseStopsRun(t *testing.T) {
	q := b.newQueue(t)
	done := make(chan error, 1)
	go func() { done <- q.Run(t.Context(), nil) }()
	time.Sleep(50 * time.Millisecond)
	q.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after Close = %v, want nil", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after Close")
	}
}

func (b backend) testWorkerInApp(t *testing.T) {
	q := b.newQueue(t)
	got := make(chan metadata.Info, 1)
	worker := jobqueue.NewWorker(q, map[string]endpoint.Endpoint{
		"email": func(ctx context.Context, req any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			got <- info
			return nil, nil
		},
	})
	a := app.New(app.WithLogger(slog.New(slog.DiscardHandler)), app.WithHooks(worker))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "email", Header: metadata.Pairs("x-id", "42")}); err != nil {
		t.Fatal(err)
	}
	select {
	case info := <-got:
		if info.Kind != metadata.KindServer || info.Protocol != "jobqueue" || info.Operation != "email" ||
			info.Header.Get("x-id") != "42" {
			t.Fatalf("Info = %+v", info)
		}
	case <-time.After(waitTimeout):
		t.Fatal("job not processed")
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if s := a.State(); s != app.StateStopped {
		t.Fatalf("State() = %v, want stopped", s)
	}
}

// runSuite runs every shared test against b.
func runSuite(t *testing.T, b backend) {
	for name, fn := range map[string]func(*testing.T){
		"EnqueueAndProcess":              b.testEnqueueAndProcess,
		"Retries":                        b.testRetries,
		"UnknownJobType":                 b.testUnknownJobType,
		"Delay":                          b.testDelay,
		"Unique":                         b.testUnique,
		"UniqueWithoutTTL":               b.testUniqueWithoutTTL,
		"UniqueReclaimedAfterPanic":      b.testUniqueReclaimedAfterPanic,
		"UniqueReclaimedWhenTaskEnds":    b.testUniqueReclaimedWhenTaskEnds,
		"LateHandlerReservation":         b.testLateHandlerCannotReleaseReservation,
		"TaskIDConflict":                 b.testTaskIDConflict,
		"StopKeepsJob":                   b.testStopKeepsJob,
		"NonRetryableWhileStoppingFails": b.testNonRetryableWhileStoppingFails,
		"EnqueueErrors":                  b.testEnqueueErrors,
		"CloseStopsRun":                  b.testCloseStopsRun,
		"WorkerInApp":                    b.testWorkerInApp,
	} {
		t.Run(name, fn)
	}
}
