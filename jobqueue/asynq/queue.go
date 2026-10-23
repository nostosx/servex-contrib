package asynq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

// Compile-time interface checks.
var (
	_ jobqueue.Enqueuer = (*Queue)(nil)
	_ jobqueue.Runner   = (*Queue)(nil)
)

// errClosed is returned by Enqueue once the Queue is closed.
var errClosed = errorx.New(errorx.FailedPrecondition, "ASYNQ_CLOSED", "asynq: queue is closed")

// Scripts maintaining unique key reservations. A reservation is a Redis key
// holding the ID of the job that reserved it.
var (
	// releaseScript deletes the reservation KEYS[1] if it still belongs to
	// job ARGV[1].
	releaseScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`)
	// confirmScript turns the provisional reservation KEYS[1] of job ARGV[1],
	// whose task is now enqueued, into one that lasts until the job finishes.
	// It reserves the key again if the provisional reservation expired.
	confirmScript = redis.NewScript(`local v = redis.call("GET", KEYS[1])
if v == ARGV[1] then return redis.call("PERSIST", KEYS[1]) end
if not v then redis.call("SET", KEYS[1], ARGV[1]) return 1 end
return 0`)
	// takeOverScript reserves KEYS[1] for job ARGV[2] for ARGV[3]
	// milliseconds if it is still the job-lifetime reservation (without
	// expiry) of the finished job ARGV[1].
	takeOverScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] and redis.call("PTTL", KEYS[1]) == -1 then
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3]) return 1 end
return 0`)
)

// Queue implements jobqueue.Enqueuer and jobqueue.Runner with asynq. Its
// methods are safe for concurrent use, and several Run calls, also in
// different processes, may share the queue. Create one with New or
// NewFromClient.
//
// A unique key with a positive UniqueTTL stays reserved for that long. With a
// UniqueTTL <= 0 the key stays reserved until the job finishes: until it
// completes or fails for good, or its task is archived or deleted in another
// way, for example by asynq after a worker crashed during the last attempt or
// through an asynq.Inspector. Enqueue reclaims finished reservations lazily
// after inspecting the task's actual state; handlers never release them.
type Queue struct {
	rdb       redis.UniversalClient
	owned     bool // whether Close closes rdb
	client    *asynq.Client
	inspector *asynq.Inspector
	opts      options
	ctx       context.Context // canceled by Close
	cancel    context.CancelFunc

	mu     sync.Mutex
	closed bool
	active sync.WaitGroup // Run and Enqueue calls using rdb and client
	once   sync.Once
}

// New creates a Queue that connects to Redis as described by opt. Close
// closes the connection.
func New(opt asynq.RedisConnOpt, opts ...Option) (*Queue, error) {
	if opt == nil {
		return nil, errorx.New(errorx.InvalidArgument, "ASYNQ_INVALID_ARGUMENT", "asynq: nil redis connection option")
	}
	rdb, ok := opt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		return nil, errorx.Newf(errorx.InvalidArgument, "ASYNQ_INVALID_ARGUMENT", "asynq: unsupported redis connection option %T", opt)
	}
	q := NewFromClient(rdb, opts...)
	q.owned = true
	return q, nil
}

// NewFromClient creates a Queue that uses rdb. Close does not close rdb.
func NewFromClient(rdb redis.UniversalClient, opts ...Option) *Queue {
	o := options{
		queue:               defaultQueue,
		concurrency:         defaultConcurrency,
		shutdownTimeout:     defaultShutdownTimeout,
		healthCheckInterval: defaultHealthCheckInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.backoff == nil {
		o.backoff = defaultBackoff
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		rdb:       rdb,
		client:    asynq.NewClientFromRedisClient(rdb),
		inspector: asynq.NewInspectorFromRedisClient(rdb),
		opts:      o,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// acquire registers a Run or Enqueue call so that Close waits for it before
// closing the clients. It reports false once the Queue is closed; otherwise
// the caller must call q.active.Done when it no longer uses the clients.
func (q *Queue) acquire() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.active.Add(1)
	return true
}

// Enqueue submits a copy of job as an asynq task and returns its ID, which is
// job.ID or, when empty, a new UUIDv7. It returns an error wrapping
// jobqueue.ErrDuplicate if the job's unique key is still reserved, and an
// errorx.AlreadyExists error if a task with the same ID exists.
func (q *Queue) Enqueue(ctx context.Context, job *jobqueue.Job, opts ...jobqueue.EnqueueOption) (string, error) {
	switch {
	case job == nil:
		return "", invalid("enqueue: nil job")
	case job.Type == "":
		return "", invalid("enqueue: empty job type")
	case job.ID != "" && strings.TrimSpace(job.ID) == "":
		// asynq rejects blank task IDs.
		return "", invalid("enqueue: blank job id")
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("asynq: enqueue: %w", err)
	}
	if !q.acquire() {
		return "", errClosed
	}
	defer q.active.Done()
	o := jobqueue.ApplyOptions(opts...)
	id := job.ID
	if id == "" {
		id = uuid.NewV7().String()
	}

	var reservationKey, lifetimeKey string // lifetimeKey is set only for reservations without a positive TTL
	if o.UniqueKey != "" {
		reservationKey = q.uniqueKey(o.UniqueKey)
		ttl := o.UniqueTTL
		if ttl <= 0 {
			// Provisional until the task is enqueued, so that the reservation
			// ends if this process dies in between.
			ttl = reserveGrace
			lifetimeKey = reservationKey
		}
		ok, err := q.reserve(ctx, reservationKey, id, ttl)
		switch {
		case err != nil:
			return "", err
		case !ok:
			return "", errorx.Newf(errorx.AlreadyExists, "ASYNQ_DUPLICATE", "asynq: enqueue: unique key %q is reserved", o.UniqueKey).
				WithCause(fmt.Errorf("%w: unique key %q", jobqueue.ErrDuplicate, o.UniqueKey))
		}
	}
	headers, err := encodeHeaders(job.Header, lifetimeKey)
	if err != nil {
		q.releaseKey(ctx, reservationKey, id)
		return "", invalid("enqueue: encode header: %v", err)
	}
	task := asynq.NewTaskWithHeaders(job.Type, slices.Clone(job.Payload), headers)
	aopts := []asynq.Option{asynq.TaskID(id), asynq.Queue(q.opts.queue), asynq.MaxRetry(o.MaxRetry)}
	if o.Delay > 0 {
		aopts = append(aopts, asynq.ProcessIn(o.Delay))
	}
	if _, err := q.client.EnqueueContext(ctx, task, aopts...); err != nil {
		q.releaseKey(ctx, reservationKey, id)
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			return "", errorx.Newf(errorx.AlreadyExists, "ASYNQ_TASK_ID_CONFLICT", "asynq: enqueue: task %q already exists", id).WithCause(err)
		}
		return "", classify(ctx, "enqueue", err)
	}
	if lifetimeKey != "" {
		q.confirm(ctx, lifetimeKey, id)
	}
	return id, nil
}

// reserve reserves key for job id for ttl and reports whether it succeeded.
// A job-lifetime reservation (one without expiry) whose job has finished
// without releasing it is taken over.
func (q *Queue) reserve(ctx context.Context, key, id string, ttl time.Duration) (bool, error) {
	ok, err := q.rdb.SetNX(ctx, key, id, ttl).Result()
	if err != nil || ok {
		return ok, classifyReserve(ctx, err)
	}
	var holder *redis.StringCmd
	var pttl *redis.DurationCmd
	if _, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		holder = p.Get(ctx, key)
		pttl = p.PTTL(ctx, key)
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) {
		return false, classifyReserve(ctx, err)
	}
	switch {
	case errors.Is(holder.Err(), redis.Nil):
		// Released in the meantime.
		ok, err := q.rdb.SetNX(ctx, key, id, ttl).Result()
		return ok, classifyReserve(ctx, err)
	case pttl.Val() != -1:
		// A reservation with a TTL, or a provisional one whose task is being
		// enqueued.
		return false, nil
	}
	finished, err := q.finished(ctx, holder.Val())
	if err != nil || !finished {
		return false, err
	}
	ok, err = takeOverScript.Run(ctx, q.rdb, []string{key}, holder.Val(), id, ttl.Milliseconds()).Bool()
	return ok, classifyReserve(ctx, err)
}

// classifyReserve classifies an error of reserve; nil stays nil.
func classifyReserve(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return classify(ctx, "enqueue: reserve unique key", err)
}

// finished reports whether the task of job id no longer exists or was
// archived or completed, so that its job-lifetime reservation is over.
func (q *Queue) finished(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("asynq: enqueue: %w", err)
	}
	info, err := q.inspector.GetTaskInfo(q.opts.queue, id)
	switch {
	case errors.Is(err, asynq.ErrTaskNotFound), errors.Is(err, asynq.ErrQueueNotFound):
		return true, nil
	case err != nil:
		return false, classify(ctx, "enqueue: inspect reserving task", err)
	}
	return info.State == asynq.TaskStateArchived || info.State == asynq.TaskStateCompleted, nil
}

// confirm makes the provisional reservation of key by job id, whose task was
// enqueued, last until the job finishes. On failure the reservation ends
// with its provisional expiry.
func (q *Queue) confirm(ctx context.Context, key, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := confirmScript.Run(ctx, q.rdb, []string{key}, id).Err(); err != nil {
		q.opts.logger.LogAttrs(ctx, slog.LevelWarn, "asynq: could not confirm unique key reservation",
			slog.String("key", key), slog.String("job_id", id), slog.Any("error", err))
	}
}

// Run processes jobs of the queue with up to the configured number of
// concurrent workers until ctx ends or the Queue is closed, returning nil in
// both cases. It returns a retryable errorx.Unavailable error when Redis
// cannot be reached at start or stays unreachable (see
// WithHealthCheckInterval).
//
// Delays are scheduled by asynq with a precision of one second.
//
// Once ctx ended, running handlers may continue for the shutdown timeout;
// then their context is canceled and their jobs are put back into the queue
// with the same attempt number. A handler that returns a retryable error
// while Run is stopping, or that was cut short, does not use up an attempt
// either. A job whose Type has no handler fails immediately, and so does a
// job whose handler panics: the panic is logged and reported as a
// non-retryable errorx.Internal error with reason ASYNQ_PANIC.
func (q *Queue) Run(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
	if !q.acquire() {
		return nil
	}
	defer q.active.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(q.ctx, cancel)
	defer stop()

	if err := q.rdb.Ping(ctx).Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return classify(context.Background(), "run: ping", err)
	}

	lost := make(chan error, 1)
	failures := 0
	cfg := asynq.Config{
		Concurrency: q.opts.concurrency,
		Queues:      map[string]int{q.opts.queue: 1},
		BaseContext: func() context.Context { return context.WithoutCancel(ctx) },
		RetryDelayFunc: func(n int, _ error, _ *asynq.Task) time.Duration {
			return max(q.opts.backoff(n+1), 0)
		},
		ShutdownTimeout:     q.opts.shutdownTimeout,
		HealthCheckInterval: q.opts.healthCheckInterval,
		HealthCheckFunc: func(err error) {
			if err == nil {
				failures = 0
				return
			}
			if failures++; failures >= maxHealthCheckFailures {
				select {
				case lost <- classify(context.Background(), "run: connection lost", err):
				default:
				}
			}
		},
		Logger:   logger{q.opts.logger},
		LogLevel: asynq.WarnLevel,
	}
	if fn := q.opts.configure; fn != nil {
		fn(&cfg)
	}
	srv := asynq.NewServerFromRedisClient(q.rdb, cfg)
	if err := srv.Start(q.handler(ctx, maps.Clone(handlers))); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return classify(context.Background(), "run: start", err)
	}
	var err error
	select {
	case <-ctx.Done():
	case err = <-lost:
	}
	srv.Shutdown()
	return err
}

// Close closes the Queue. Afterwards Enqueue returns an error and Run returns
// nil. Close waits until active Run calls returned, which they do once their
// handlers returned or the shutdown timeout ended, and until active Enqueue
// calls returned; then it closes the Redis client if the Queue was created
// with New. Close is idempotent and always returns nil.
func (q *Queue) Close() error {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		q.cancel()
		// The asynq servers of active Run calls use rdb until they shut down.
		q.active.Wait()
		_ = q.client.Close()
		if q.owned {
			_ = q.rdb.Close()
		}
	})
	return nil
}

// handler returns the asynq handler that dispatches tasks to handlers. runCtx
// is the context of Run.
func (q *Queue) handler(runCtx context.Context, handlers map[string]endpoint.Endpoint) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		job, _ := toJob(ctx, t)
		h := handlers[t.Type()]
		if h == nil {
			err := fmt.Errorf("asynq: no handler registered for job type %q", t.Type())
			q.fail(job, err)
			return fmt.Errorf("%w: %w", err, asynq.SkipRetry)
		}
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		err := q.call(ctx, h, job)
		switch {
		case err == nil:
			// asynq settles the task after this handler returns and may have
			// already retried it after cancellation. Only reserve may reclaim
			// the reservation, after observing the actual terminal task state.
			return nil
		case interrupted(runCtx, err):
			return requeue(ctx)
		case !errorx.IsRetryable(err):
			// An explicit rejection fails the job even while Run is stopping.
			q.fail(job, err)
			return fmt.Errorf("%w: %w", err, asynq.SkipRetry)
		case runCtx.Err() != nil:
			// Run is stopping: retry in the next Run without using up an
			// attempt.
			return requeue(ctx)
		case job.Attempt <= maxRetry:
			return err
		default:
			q.fail(job, err)
			return err
		}
	}
}

// call calls h with job and converts a panic into a non-retryable error, so
// that the job fails for good.
func (q *Queue) call(ctx context.Context, h endpoint.Endpoint, job *jobqueue.Job) (err error) {
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		q.opts.logger.LogAttrs(ctx, slog.LevelError, "asynq: job handler panicked",
			slog.String("job_id", job.ID), slog.String("job_type", job.Type),
			slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
		err = errorx.New(errorx.Internal, "ASYNQ_PANIC", "asynq: job handler panicked").
			WithCause(fmt.Errorf("panic: %v", p)).WithRetryable(false)
	}()
	_, err = h(ctx, job)
	return err
}

// requeue waits until asynq gives up on the running task at the end of the
// shutdown timeout, which puts the task back into the queue without counting
// an attempt, and returns the context error.
func requeue(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// fail reports the handler's final failure decision. asynq still owns the
// settlement; the reservation remains until reserve observes that settlement.
func (q *Queue) fail(job *jobqueue.Job, err error) {
	if fn := q.opts.failed; fn != nil {
		fn(job, err)
	}
}

// releaseKey drops the reservation key if it still belongs to job id.
func (q *Queue) releaseKey(ctx context.Context, key, id string) {
	if key == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = releaseScript.Run(ctx, q.rdb, []string{key}, id).Err()
}

// uniqueKey returns the Redis key that reserves the unique key k.
func (q *Queue) uniqueKey(k string) string {
	return uniquePrefix + q.opts.queue + ":" + k
}

// interrupted reports whether err means that the attempt was cut short by the
// end of Run rather than settled by the handler.
func interrupted(runCtx context.Context, err error) bool {
	return runCtx.Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) == errorx.Canceled)
}
