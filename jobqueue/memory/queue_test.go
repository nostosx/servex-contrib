package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
)

// Compile-time interface checks.
var (
	_ jobqueue.Enqueuer = (*Queue)(nil)
	_ jobqueue.Runner   = (*Queue)(nil)
)

var errUnavailable = errorx.New(errorx.Unavailable, "DOWN", "service down")

// run starts q.Run in the background and returns a function that stops it and
// returns its result.
func run(t *testing.T, q *Queue, handlers map[string]endpoint.Endpoint) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var err error
	go func() {
		err = q.Run(ctx, handlers)
		close(done)
	}()
	stop = func() error {
		cancel()
		<-done
		return err
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
	synctest.Wait()
	return stop
}

func enqueue(t *testing.T, q *Queue, job *jobqueue.Job, opts ...jobqueue.EnqueueOption) string {
	t.Helper()
	id, err := q.Enqueue(t.Context(), job, opts...)
	if err != nil {
		t.Fatalf("Enqueue(%+v) = %v", job, err)
	}
	return id
}

// failure is a job reported through WithFailed.
type failure struct {
	job jobqueue.Job
	err error
}

// failures collects WithFailed reports.
type failures struct {
	mu   sync.Mutex
	list []failure
}

func (f *failures) option() Option {
	return WithFailed(func(job *jobqueue.Job, err error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.list = append(f.list, failure{*job, err})
	})
}

func (f *failures) get() []failure {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.list)
}

func TestEnqueueIDs(t *testing.T) {
	q := New()
	defer q.Close()
	job := &jobqueue.Job{Type: "email"}
	id := enqueue(t, q, job)
	u, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("generated ID %q is not a UUID: %v", id, err)
	}
	if v := u[6] >> 4; v != 7 {
		t.Fatalf("generated ID %q has version %d, want 7", id, v)
	}
	if job.ID != "" || job.Attempt != 0 {
		t.Fatalf("Enqueue modified the caller's job: %+v", job)
	}
	if id2 := enqueue(t, q, &jobqueue.Job{Type: "email"}); id2 == id {
		t.Fatal("generated IDs are not unique")
	}
	if got := enqueue(t, q, &jobqueue.Job{ID: "custom", Type: "email"}); got != "custom" {
		t.Fatalf("Enqueue() = %q, want the caller's ID", got)
	}
}

func TestEnqueueErrors(t *testing.T) {
	q := New()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name string
		ctx  context.Context
		job  *jobqueue.Job
		is   error
	}{
		{"nil job", t.Context(), nil, nil},
		{"empty type", t.Context(), &jobqueue.Job{}, nil},
		{"canceled context", canceled, &jobqueue.Job{Type: "x"}, context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := q.Enqueue(tt.ctx, tt.job)
			if err == nil || !strings.HasPrefix(err.Error(), "memory: ") {
				t.Fatalf("Enqueue() = %v, want memory error", err)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("Enqueue() = %v, want %v", err, tt.is)
			}
		})
	}
	q.Close()
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}); !errors.Is(err, errClosed) {
		t.Fatalf("Enqueue() after Close = %v, want closed error", err)
	}
}

func TestRunProcessesJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		got := make(chan jobqueue.Job, 1)
		stop := run(t, q, map[string]endpoint.Endpoint{
			"email": func(_ context.Context, req any) (any, error) {
				got <- *req.(*jobqueue.Job)
				return nil, nil
			},
		})
		job := &jobqueue.Job{Type: "email", Payload: []byte("hi"), Header: metadata.Pairs("k", "v")}
		id := enqueue(t, q, job)
		job.Payload[0] = 'X'
		job.Header.Set("k", "changed")

		j := <-got
		if j.ID != id || j.Type != "email" || string(j.Payload) != "hi" || j.Header.Get("k") != "v" || j.Attempt != 1 {
			t.Fatalf("handler got %+v", j)
		}
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
	})
}

func TestConcurrency(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want int
	}{
		{"default", nil, 4},
		{"two", []Option{WithConcurrency(2)}, 2},
		{"below one", []Option{WithConcurrency(0)}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := New(tt.opts...)
				var inFlight, peak, done atomic.Int32
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				stop := run(t, q, map[string]endpoint.Endpoint{
					"slow": func(context.Context, any) (any, error) {
						n := inFlight.Add(1)
						for {
							p := peak.Load()
							if n <= p || peak.CompareAndSwap(p, n) {
								break
							}
						}
						<-release
						inFlight.Add(-1)
						done.Add(1)
						return nil, nil
					},
				})
				const jobs = 6
				for range jobs {
					enqueue(t, q, &jobqueue.Job{Type: "slow"})
				}
				synctest.Wait()
				if got := inFlight.Load(); int(got) != tt.want {
					t.Fatalf("jobs in flight = %d, want %d", got, tt.want)
				}
				unblock()
				synctest.Wait()
				if done.Load() != jobs || int(peak.Load()) != tt.want {
					t.Fatalf("done = %d, peak = %d; want %d, %d", done.Load(), peak.Load(), jobs, tt.want)
				}
				if err := stop(); err != nil {
					t.Errorf("Run() = %v, want nil", err)
				}
			})
		})
	}
}

func TestDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New(WithConcurrency(1))
		var mu sync.Mutex
		var order []string
		var ranAt []time.Duration
		start := time.Now()
		stop := run(t, q, map[string]endpoint.Endpoint{
			"job": func(_ context.Context, req any) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, string(req.(*jobqueue.Job).Payload))
				ranAt = append(ranAt, time.Since(start))
				return nil, nil
			},
		})
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("late")}, jobqueue.WithDelay(5*time.Second))
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("soon")}, jobqueue.WithDelay(2*time.Second))
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("same")}, jobqueue.WithDelay(2*time.Second))
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("now")})

		synctest.Wait()
		synctest.Sleep(5*time.Second - time.Nanosecond)
		mu.Lock()
		before := slices.Clone(order)
		mu.Unlock()
		if want := []string{"now", "soon", "same"}; !slices.Equal(before, want) {
			t.Fatalf("before 5s: order = %v, want %v", before, want)
		}
		synctest.Sleep(time.Nanosecond)
		mu.Lock()
		after, times := slices.Clone(order), slices.Clone(ranAt)
		mu.Unlock()
		if want := []string{"now", "soon", "same", "late"}; !slices.Equal(after, want) {
			t.Fatalf("after 5s: order = %v, want %v", after, want)
		}
		if want := []time.Duration{0, 2 * time.Second, 2 * time.Second, 5 * time.Second}; !slices.Equal(times, want) {
			t.Fatalf("run times = %v, want %v", times, want)
		}
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestDelayedJobEnqueuedWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New(WithConcurrency(1))
		ran := make(chan string, 2)
		stop := run(t, q, map[string]endpoint.Endpoint{
			"job": func(_ context.Context, req any) (any, error) {
				ran <- string(req.(*jobqueue.Job).Payload)
				return nil, nil
			},
		})
		start := time.Now()
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("10s")}, jobqueue.WithDelay(10*time.Second))
		synctest.Sleep(time.Second)
		// An earlier deadline arriving while the worker waits is honoured.
		enqueue(t, q, &jobqueue.Job{Type: "job", Payload: []byte("3s")}, jobqueue.WithDelay(2*time.Second))
		if got := <-ran; got != "3s" || time.Since(start) != 3*time.Second {
			t.Fatalf("first job %q at %v, want 3s at 3s", got, time.Since(start))
		}
		if got := <-ran; got != "10s" || time.Since(start) != 10*time.Second {
			t.Fatalf("second job %q at %v, want 10s at 10s", got, time.Since(start))
		}
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestRetries(t *testing.T) {
	linear := WithBackoff(func(attempt int) time.Duration { return time.Duration(attempt) * time.Second })
	tests := []struct {
		name         string
		opts         []Option
		enqueue      []jobqueue.EnqueueOption
		failures     int
		err          error
		wantAttempts []int
		wantTimes    []time.Duration
		wantFailed   bool
	}{
		{
			name: "default max retry and backoff", failures: 100, err: errUnavailable,
			wantAttempts: []int{1, 2, 3, 4},
			wantTimes:    []time.Duration{0, 100 * time.Millisecond, 300 * time.Millisecond, 700 * time.Millisecond},
			wantFailed:   true,
		},
		{
			name: "custom backoff and max retry", opts: []Option{linear},
			enqueue: []jobqueue.EnqueueOption{jobqueue.WithMaxRetry(2)}, failures: 100, err: errUnavailable,
			wantAttempts: []int{1, 2, 3},
			wantTimes:    []time.Duration{0, time.Second, 3 * time.Second},
			wantFailed:   true,
		},
		{
			name: "succeeds after retries", opts: []Option{linear}, failures: 2, err: errUnavailable,
			wantAttempts: []int{1, 2, 3},
			wantTimes:    []time.Duration{0, time.Second, 3 * time.Second},
		},
		{
			name: "no retries", enqueue: []jobqueue.EnqueueOption{jobqueue.WithMaxRetry(0)}, failures: 100, err: errUnavailable,
			wantAttempts: []int{1}, wantTimes: []time.Duration{0}, wantFailed: true,
		},
		{
			name: "non-retryable fails immediately", failures: 100, err: errors.New("bad input"),
			wantAttempts: []int{1}, wantTimes: []time.Duration{0}, wantFailed: true,
		},
		{
			name: "explicitly non-retryable", failures: 100, err: errUnavailable.WithRetryable(false),
			wantAttempts: []int{1}, wantTimes: []time.Duration{0}, wantFailed: true,
		},
		{
			name: "wrapped retryable", opts: []Option{linear}, enqueue: []jobqueue.EnqueueOption{jobqueue.WithMaxRetry(1)},
			failures: 100, err: fmt.Errorf("call: %w", errUnavailable),
			wantAttempts: []int{1, 2}, wantTimes: []time.Duration{0, time.Second}, wantFailed: true,
		},
		{
			name: "negative backoff retries at once", opts: []Option{WithBackoff(func(int) time.Duration { return -time.Second })},
			enqueue: []jobqueue.EnqueueOption{jobqueue.WithMaxRetry(1)}, failures: 100, err: errUnavailable,
			wantAttempts: []int{1, 2}, wantTimes: []time.Duration{0, 0}, wantFailed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var failed failures
				q := New(append(slices.Clone(tt.opts), failed.option())...)
				var attempts []int
				var times []time.Duration
				var seen []*jobqueue.Job
				start := time.Now()
				stop := run(t, q, map[string]endpoint.Endpoint{
					"job": func(_ context.Context, req any) (any, error) {
						job := req.(*jobqueue.Job)
						seen = append(seen, job)
						attempts = append(attempts, job.Attempt)
						times = append(times, time.Since(start))
						if len(attempts) <= tt.failures {
							return nil, tt.err
						}
						return nil, nil
					},
				})
				id := enqueue(t, q, &jobqueue.Job{Type: "job"}, tt.enqueue...)
				synctest.Sleep(time.Minute)

				if !slices.Equal(attempts, tt.wantAttempts) {
					t.Errorf("attempts = %v, want %v", attempts, tt.wantAttempts)
				}
				if !slices.Equal(times, tt.wantTimes) {
					t.Errorf("attempt times = %v, want %v", times, tt.wantTimes)
				}
				for i := 1; i < len(seen); i++ {
					if seen[i] == seen[i-1] || seen[i].ID != id {
						t.Errorf("attempt %d reused the job value or changed its ID", i+1)
					}
				}
				got := failed.get()
				if !tt.wantFailed {
					if len(got) != 0 {
						t.Fatalf("failed = %+v, want none", got)
					}
				} else {
					if len(got) != 1 {
						t.Fatalf("failed called %d times, want 1", len(got))
					}
					last := tt.wantAttempts[len(tt.wantAttempts)-1]
					if got[0].job.ID != id || got[0].job.Attempt != last || !errors.Is(got[0].err, tt.err) {
						t.Fatalf("failed = %+v, want job %s attempt %d error %v", got[0], id, last, tt.err)
					}
				}
				if err := stop(); err != nil {
					t.Errorf("Run() = %v, want nil", err)
				}
			})
		})
	}
}

func TestDefaultBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{math.MinInt, 100 * time.Millisecond},
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{7, 6400 * time.Millisecond},
		{8, 10 * time.Second},
		{100, 10 * time.Second},
		{math.MaxInt, 10 * time.Second},
	}
	for _, tt := range tests {
		if got := defaultBackoff(tt.attempt); got != tt.want {
			t.Errorf("defaultBackoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
	q := New(WithBackoff(nil), WithClock(nil))
	if q.opts.backoff == nil || q.opts.now == nil {
		t.Fatal("nil backoff or clock was not replaced by the default")
	}
}

func TestUnknownJobType(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failed failures
		q := New(failed.option())
		var calls atomic.Int32
		stop := run(t, q, map[string]endpoint.Endpoint{
			"known": func(context.Context, any) (any, error) {
				calls.Add(1)
				return nil, nil
			},
		})
		id := enqueue(t, q, &jobqueue.Job{Type: "mystery"})
		synctest.Wait()
		got := failed.get()
		if len(got) != 1 || got[0].job.ID != id || got[0].job.Attempt != 1 {
			t.Fatalf("failed = %+v, want job %s at attempt 1", got, id)
		}
		if msg := got[0].err.Error(); !strings.Contains(msg, `no handler`) || !strings.Contains(msg, `"mystery"`) {
			t.Fatalf("error %q does not explain the unknown type", msg)
		}
		if calls.Load() != 0 {
			t.Fatal("a handler was called for an unknown type")
		}
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestUnique(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		defer q.Close()
		ttl := time.Minute
		enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", ttl))
		enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("other", ttl))
		enqueue(t, q, &jobqueue.Job{Type: "x"}) // no key, never a duplicate

		time.Sleep(ttl - time.Nanosecond)
		_, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", ttl))
		if !errors.Is(err, jobqueue.ErrDuplicate) {
			t.Fatalf("Enqueue() within TTL = %v, want ErrDuplicate", err)
		}
		time.Sleep(time.Nanosecond)
		enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", ttl))
	})
}

func TestUniqueWithoutTTLLastsUntilFinished(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"completed", nil},
		{"failed", errors.New("bad")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := New()
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				stop := run(t, q, map[string]endpoint.Endpoint{
					"x": func(context.Context, any) (any, error) {
						<-release
						return nil, tt.err
					},
				})
				enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", 0))
				time.Sleep(time.Hour)
				if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
					t.Fatalf("Enqueue() while running = %v, want ErrDuplicate", err)
				}
				unblock()
				synctest.Wait()
				enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", 0))
				synctest.Wait()
				if err := stop(); err != nil {
					t.Errorf("Run() = %v, want nil", err)
				}
			})
		})
	}
}

func TestExpiredUniqueLeaseCannotReleaseNewReservation(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"completed", nil},
		{"failed", errors.New("rejected")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := New(WithConcurrency(1))
				defer q.Close()
				oldRelease, newRelease := make(chan struct{}), make(chan struct{})
				started := make(chan string, 2)
				stop := run(t, q, map[string]endpoint.Endpoint{
					"x": func(ctx context.Context, req any) (any, error) {
						payload := string(req.(*jobqueue.Job).Payload)
						started <- payload
						if payload == "old" {
							select {
							case <-oldRelease:
								return nil, tt.err
							case <-ctx.Done():
								return nil, ctx.Err()
							}
						}
						select {
						case <-newRelease:
							return nil, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
				})
				defer func() {
					if err := stop(); err != nil {
						t.Errorf("Run() = %v", err)
					}
				}()
				enqueue(t, q, &jobqueue.Job{ID: "same-id", Type: "x", Payload: []byte("old")}, jobqueue.WithUnique("k", time.Second))
				if got := <-started; got != "old" {
					t.Fatalf("first job = %q", got)
				}
				synctest.Sleep(time.Second)
				enqueue(t, q, &jobqueue.Job{ID: "same-id", Type: "x", Payload: []byte("new")}, jobqueue.WithUnique("k", 0))
				close(oldRelease)
				if got := <-started; got != "new" {
					t.Fatalf("second job = %q", got)
				}
				if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
					t.Fatalf("Enqueue() while new reservation is running = %v, want ErrDuplicate", err)
				}
				close(newRelease)
				synctest.Wait()
				enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", 0))
				synctest.Wait()
			})
		})
	}
}

func TestWithClock(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	q := New(WithClock(clock))
	defer q.Close()
	enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", time.Hour))
	advance(59 * time.Minute)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", time.Hour)); !errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatalf("Enqueue() = %v, want ErrDuplicate", err)
	}
	advance(time.Minute)
	enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithUnique("k", time.Hour))

	// Delayed jobs are scheduled against the configured clock.
	enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithDelay(time.Minute))
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.delayed) != 1 || !q.delayed[0].runAt.Equal(clock().Add(time.Minute)) {
		t.Fatalf("delayed job not scheduled one minute after the clock's now")
	}
}

func TestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		canceled := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- q.Run(t.Context(), map[string]endpoint.Endpoint{
				"x": func(ctx context.Context, _ any) (any, error) {
					<-ctx.Done()
					close(canceled)
					return nil, ctx.Err()
				},
			})
		}()
		enqueue(t, q, &jobqueue.Job{Type: "x"})
		enqueue(t, q, &jobqueue.Job{Type: "x"}, jobqueue.WithDelay(time.Hour))
		synctest.Wait()
		if err := q.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		<-canceled
		if err := <-done; err != nil {
			t.Fatalf("Run() after Close = %v, want nil", err)
		}
		if err := q.Close(); err != nil {
			t.Fatalf("second Close() = %v", err)
		}
		if err := q.Run(t.Context(), nil); err != nil {
			t.Fatalf("Run() on a closed queue = %v, want nil", err)
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		if len(q.ready) != 0 || len(q.delayed) != 0 {
			t.Fatal("pending jobs not discarded by Close")
		}
	})
}

func TestStoppedRunKeepsInterruptedJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failed failures
		q := New(failed.option())
		started := make(chan struct{})
		stop := run(t, q, map[string]endpoint.Endpoint{
			"x": func(ctx context.Context, _ any) (any, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})
		id := enqueue(t, q, &jobqueue.Job{Type: "x"})
		<-started
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}

		got := make(chan jobqueue.Job, 1)
		stop = run(t, q, map[string]endpoint.Endpoint{
			"x": func(_ context.Context, req any) (any, error) {
				got <- *req.(*jobqueue.Job)
				return nil, nil
			},
		})
		if j := <-got; j.ID != id || j.Attempt != 1 {
			t.Fatalf("resumed job = %+v, want ID %s attempt 1", j, id)
		}
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
		if f := failed.get(); len(f) != 0 {
			t.Fatalf("interrupted job reported as failed: %+v", f)
		}
	})
}

func TestNonRetryableErrorWhileStoppingFailsJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failed failures
		q := New(failed.option())
		started := make(chan struct{})
		stop := run(t, q, map[string]endpoint.Endpoint{
			"x": func(ctx context.Context, _ any) (any, error) {
				close(started)
				<-ctx.Done()
				// The handler rejects the job explicitly after stop began.
				return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad job")
			},
		})
		id := enqueue(t, q, &jobqueue.Job{Type: "x"})
		<-started
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		f := failed.get()
		if len(f) != 1 || f[0].job.ID != id || errorx.CodeOf(f[0].err) != errorx.InvalidArgument {
			t.Fatalf("failures = %+v, want the job failed with InvalidArgument", f)
		}

		// The job must not come back in the next Run.
		var calls atomic.Int32
		stop = run(t, q, map[string]endpoint.Endpoint{
			"x": func(context.Context, any) (any, error) { calls.Add(1); return nil, nil },
		})
		synctest.Wait()
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("failed job ran again %d times", n)
		}
	})
}

func TestSingleRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		stop := run(t, q, nil)
		if err := q.Run(t.Context(), nil); !errors.Is(err, errRunning) {
			t.Fatalf("second Run() = %v, want already running", err)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		// A new Run may start after the previous one returned.
		stop = run(t, q, nil)
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestConcurrentEnqueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New(WithConcurrency(3))
		var mu sync.Mutex
		seen := make(map[string]int)
		stop := run(t, q, map[string]endpoint.Endpoint{
			"x": func(_ context.Context, req any) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				seen[req.(*jobqueue.Job).ID]++
				return nil, nil
			},
		})
		const producers, perProducer = 8, 40
		var wg sync.WaitGroup
		for range producers {
			wg.Go(func() {
				for i := range perProducer {
					var opts []jobqueue.EnqueueOption
					if i%3 == 0 {
						opts = append(opts, jobqueue.WithDelay(time.Duration(i)*time.Millisecond))
					}
					if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "x"}, opts...); err != nil {
						t.Errorf("Enqueue() = %v", err)
					}
				}
			})
		}
		wg.Wait()
		synctest.Sleep(time.Second)
		mu.Lock()
		if len(seen) != producers*perProducer {
			t.Errorf("processed %d distinct jobs, want %d", len(seen), producers*perProducer)
		}
		for id, n := range seen {
			if n != 1 {
				t.Errorf("job %s processed %d times", id, n)
			}
		}
		mu.Unlock()
		if err := stop(); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	})
}

func TestWithWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		got := make(chan metadata.Info, 1)
		w := jobqueue.NewWorker(q, map[string]endpoint.Endpoint{
			"email": func(ctx context.Context, _ any) (any, error) {
				info, _ := metadata.FromContext(ctx)
				got <- info
				return nil, nil
			},
		})
		if err := w.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		enqueue(t, q, &jobqueue.Job{Type: "email", Header: metadata.Pairs("tenant", "acme")})
		info := <-got
		if info.Protocol != "jobqueue" || info.Operation != "email" || info.Header.Get("tenant") != "acme" {
			t.Fatalf("Info = %+v", info)
		}
		if err := w.Stop(t.Context()); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
	})
}
