package asynq

import (
	"log/slog"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nostosx/servex/messaging/jobqueue"
)

const (
	defaultQueue               = "default"
	defaultConcurrency         = 4
	defaultShutdownTimeout     = 8 * time.Second
	defaultHealthCheckInterval = 15 * time.Second
	// maxHealthCheckFailures is the number of consecutive failed Redis health
	// checks after which Run reports the connection as lost.
	maxHealthCheckFailures = 3
	backoffBase            = 100 * time.Millisecond
	backoffCap             = 10 * time.Second
	// uniquePrefix prefixes the Redis keys that reserve unique keys.
	uniquePrefix = "servex:jobqueue:unique:"
	// reserveGrace bounds a reservation lasting until the job finishes
	// (UniqueTTL <= 0) until the job's task is enqueued.
	reserveGrace = time.Minute
)

// Option configures a Queue.
type Option func(*options)

type options struct {
	queue               string
	concurrency         int
	backoff             func(attempt int) time.Duration
	failed              func(job *jobqueue.Job, err error)
	shutdownTimeout     time.Duration
	healthCheckInterval time.Duration
	logger              *slog.Logger
	configure           func(*asynq.Config)
}

// WithServerConfig sets a function that adjusts the asynq server
// configuration built by Run, for settings without a dedicated option, such
// as TaskCheckInterval or DelayedTaskCheckInterval. Changing Queues,
// BaseContext, RetryDelayFunc or HealthCheckFunc breaks the Queue semantics.
func WithServerConfig(fn func(cfg *asynq.Config)) Option {
	return func(o *options) { o.configure = fn }
}

// WithQueue sets the asynq queue jobs are enqueued to and processed from. The
// default is "default"; a blank name, which asynq rejects, is ignored.
func WithQueue(name string) Option {
	return func(o *options) {
		if strings.TrimSpace(name) != "" {
			o.queue = name
		}
	}
}

// WithConcurrency sets how many jobs Run processes in parallel. The default
// is 4; values below 1 are treated as 1.
func WithConcurrency(n int) Option {
	return func(o *options) { o.concurrency = max(n, 1) }
}

// WithBackoff sets the delay before retrying a job whose attempt number
// attempt (starting at 1) failed with a retryable error. The default doubles
// from 100ms per attempt, capped at 10s. A nil fn restores the default.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(o *options) { o.backoff = fn }
}

// WithFailed sets the function that receives jobs that failed for good,
// together with the last error: jobs rejected with a non-retryable error,
// jobs that exhausted their retries and jobs without a registered handler.
// It is called synchronously from the worker goroutine before asynq archives
// the task.
func WithFailed(fn func(job *jobqueue.Job, err error)) Option {
	return func(o *options) { o.failed = fn }
}

// WithShutdownTimeout sets how long running handlers may continue once Run's
// context ended. Afterwards their context is canceled and their jobs are put
// back into the queue with the same attempt number. The default is 8s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = d }
}

// WithHealthCheckInterval sets how often Run checks the Redis connection.
// After three consecutive failed checks Run returns a retryable
// errorx.Unavailable error. The default is 15s.
func WithHealthCheckInterval(d time.Duration) Option {
	return func(o *options) { o.healthCheckInterval = d }
}

// WithLogger sets the logger of the asynq server; only warnings and errors
// are logged. The default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// defaultBackoff doubles from backoffBase per attempt and is capped at
// backoffCap.
func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		return backoffBase
	}
	return min(backoffBase<<min(attempt-1, 8), backoffCap)
}
