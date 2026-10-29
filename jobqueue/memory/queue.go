package memory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

var (
	// errClosed is returned by Enqueue once the Queue is closed.
	errClosed = errors.New("memory: queue is closed")
	// errRunning is returned by Run while another Run is active.
	errRunning = errors.New("memory: queue is already running")
)

// Queue is an in-memory implementation of jobqueue.Enqueuer,
// jobqueue.Runner and jobqueue.ReadyRunner. Its methods are safe for concurrent use. The zero value is
// not usable; create one with New.
//
// Only one Run may be active at a time. A job whose handler is interrupted
// because Run's context ended is put back at the front of the queue with the
// same attempt number and is picked up by the next Run.
//
// A unique key with a positive UniqueTTL stays reserved for that long. With a
// UniqueTTL <= 0 the key stays reserved until the job completes or fails.
type Queue struct {
	opts   options
	ctx    context.Context // canceled by Close
	cancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	running bool
	seq     uint64
	ready   []*entry          // FIFO of due jobs
	delayed []*entry          // sorted by (runAt, seq)
	unique  map[string]*lease // reserved unique keys
	wake    chan struct{}     // closed and replaced whenever jobs change
}

// entry is a job waiting in the queue.
type entry struct {
	job   *jobqueue.Job
	opts  jobqueue.EnqueueOptions
	runAt time.Time
	seq   uint64
	lease *lease // reservation identity, independent of the caller's job ID
}

// lease is the reservation of a unique key.
type lease struct {
	expires time.Time // zero: until the job completes or fails
}

// New creates a Queue.
func New(opts ...Option) *Queue {
	o := options{concurrency: defaultConcurrency}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.backoff == nil {
		o.backoff = defaultBackoff
	}
	if o.now == nil {
		o.now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		opts:   o,
		ctx:    ctx,
		cancel: cancel,
		unique: make(map[string]*lease),
		wake:   make(chan struct{}),
	}
}

// Enqueue adds a copy of job to the queue and returns its ID. An empty
// job.ID is replaced by a new UUIDv7 in the copy; job itself is not modified.
// It returns jobqueue.ErrDuplicate if the job's unique key is still reserved.
func (q *Queue) Enqueue(ctx context.Context, job *jobqueue.Job, opts ...jobqueue.EnqueueOption) (string, error) {
	switch {
	case job == nil:
		return "", errors.New("memory: enqueue: nil job")
	case job.Type == "":
		return "", errors.New("memory: enqueue: empty job type")
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("memory: enqueue: %w", err)
	}
	o := jobqueue.ApplyOptions(opts...)
	j := &jobqueue.Job{
		ID:      job.ID,
		Type:    job.Type,
		Payload: slices.Clone(job.Payload),
		Header:  job.Header.Clone(),
		Attempt: 1,
	}
	if j.ID == "" {
		j.ID = uuid.NewV7().String()
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return "", errClosed
	}
	now := q.opts.now()
	var reservation *lease
	if o.UniqueKey != "" {
		if l := q.unique[o.UniqueKey]; l != nil && (l.expires.IsZero() || now.Before(l.expires)) {
			return "", fmt.Errorf("%w: unique key %q", jobqueue.ErrDuplicate, o.UniqueKey)
		}
		l := &lease{}
		if o.UniqueTTL > 0 {
			l.expires = now.Add(o.UniqueTTL)
		}
		q.unique[o.UniqueKey] = l
		reservation = l
		q.pruneLeasesLocked(now)
	}
	q.scheduleLocked(&entry{job: j, opts: o, lease: reservation}, now.Add(o.Delay), now)
	return j.ID, nil
}

// Run processes jobs with up to the configured number of concurrent workers
// until ctx ends or the Queue is closed, returning nil in both cases. It waits
// for running handlers to return before it returns. Handlers receive a
// context that is canceled when Run stops. A job whose Type has no handler
// fails immediately. Run returns an error if another Run is active.
func (q *Queue) Run(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
	return q.RunReady(ctx, handlers, nil)
}

// RunReady behaves like Run and implements jobqueue.ReadyRunner: it calls
// ready once its workers are processing jobs. A nil ready is allowed. It
// returns errRunning without calling ready while another Run is active.
func (q *Queue) RunReady(ctx context.Context, handlers map[string]endpoint.Endpoint, ready func()) error {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return errRunning
	}
	q.running = true
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		q.running = false
		q.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(q.ctx, cancel)
	defer stop()

	handlers = maps.Clone(handlers)
	var wg sync.WaitGroup
	for range q.opts.concurrency {
		wg.Go(func() {
			for {
				e := q.next(ctx)
				if e == nil {
					return
				}
				q.process(ctx, handlers, e)
			}
		})
	}
	if ready != nil {
		ready()
	}
	wg.Wait()
	return nil
}

// Close closes the Queue. Afterwards Enqueue returns an error, an active Run
// returns once its handlers have returned, and pending jobs are discarded.
// Close does not wait for Run. It is idempotent and always returns nil.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.cancel()
		q.ready, q.delayed = nil, nil
		clear(q.unique)
	}
	return nil
}

// next blocks until a job is due or ctx ends, in which case it returns nil.
func (q *Queue) next(ctx context.Context) *entry {
	for {
		if ctx.Err() != nil {
			return nil
		}
		q.mu.Lock()
		// Cancellation may have happened while this worker waited for the lock.
		// Do not take an interrupted job requeued by another stopping worker.
		if q.closed || ctx.Err() != nil {
			q.mu.Unlock()
			return nil
		}
		now := q.opts.now()
		q.promoteLocked(now)
		if len(q.ready) > 0 {
			e := q.ready[0]
			q.ready[0] = nil
			q.ready = q.ready[1:]
			q.mu.Unlock()
			return e
		}
		wait := q.wake
		var timer *time.Timer
		var due <-chan time.Time
		if len(q.delayed) > 0 {
			timer = time.NewTimer(q.delayed[0].runAt.Sub(now))
			due = timer.C
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-wait:
		case <-due:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// process runs one attempt of e and settles the outcome.
func (q *Queue) process(ctx context.Context, handlers map[string]endpoint.Endpoint, e *entry) {
	job := *e.job // each attempt gets its own copy
	h := handlers[job.Type]
	if h == nil {
		q.fail(e, &job, fmt.Errorf("memory: no handler registered for job type %q", job.Type))
		return
	}
	_, err := h(ctx, &job)
	switch {
	case err == nil:
		q.finish(e)
	case interrupted(ctx, err):
		// Run is stopping and cut the attempt short: keep the job for the next Run.
		q.requeue(e)
	case !errorx.IsRetryable(err):
		// An explicit rejection fails the job even while Run is stopping.
		q.fail(e, &job, err)
	case ctx.Err() != nil:
		// Run is stopping: retry in the next Run instead of now.
		q.requeue(e)
	case job.Attempt <= e.opts.MaxRetry:
		q.retry(e, q.opts.backoff(job.Attempt))
	default:
		q.fail(e, &job, err)
	}
}

// interrupted reports whether err means that the attempt was cut short by the
// end of Run rather than settled by the handler.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) == errorx.Canceled)
}

// finish releases the resources of a completed job.
func (q *Queue) finish(e *entry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.releaseLocked(e)
}

// fail settles e as failed and reports it.
func (q *Queue) fail(e *entry, job *jobqueue.Job, err error) {
	q.finish(e)
	if q.opts.failed != nil {
		q.opts.failed(job, err)
	}
}

// retry schedules the next attempt of e after delay.
func (q *Queue) retry(e *entry, delay time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	next := *e.job
	next.Attempt++
	e.job = &next
	now := q.opts.now()
	q.scheduleLocked(e, now.Add(max(delay, 0)), now)
}

// requeue puts e back at the front of the ready queue.
func (q *Queue) requeue(e *entry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.ready = slices.Insert(q.ready, 0, e)
	q.signalLocked()
}

// scheduleLocked adds e to the queue, due at runAt, where now is the current
// time. q.mu must be held.
func (q *Queue) scheduleLocked(e *entry, runAt, now time.Time) {
	q.seq++
	e.seq, e.runAt = q.seq, runAt
	if runAt.After(now) {
		i, _ := slices.BinarySearchFunc(q.delayed, e, compareEntries)
		q.delayed = slices.Insert(q.delayed, i, e)
	} else {
		q.ready = append(q.ready, e)
	}
	q.signalLocked()
}

// promoteLocked moves delayed entries due at now to the ready queue in due
// order. q.mu must be held.
func (q *Queue) promoteLocked(now time.Time) {
	n := 0
	for n < len(q.delayed) && !q.delayed[n].runAt.After(now) {
		n++
	}
	if n > 0 {
		q.ready = append(q.ready, q.delayed[:n]...)
		q.delayed = slices.Delete(q.delayed, 0, n)
	}
}

// signalLocked wakes every idle worker. q.mu must be held.
func (q *Queue) signalLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// releaseLocked drops the unique key reservation of e if it lasts until the
// job finishes. q.mu must be held.
func (q *Queue) releaseLocked(e *entry) {
	key := e.opts.UniqueKey
	if key == "" {
		return
	}
	if l := q.unique[key]; l != nil && l == e.lease && l.expires.IsZero() {
		delete(q.unique, key)
	}
}

// pruneLeasesLocked drops expired unique key reservations. q.mu must be held.
func (q *Queue) pruneLeasesLocked(now time.Time) {
	maps.DeleteFunc(q.unique, func(_ string, l *lease) bool {
		return !l.expires.IsZero() && !now.Before(l.expires)
	})
}

// compareEntries orders entries by due time, then by scheduling order.
func compareEntries(a, b *entry) int {
	return cmp.Or(a.runAt.Compare(b.runAt), cmp.Compare(a.seq, b.seq))
}
