package memory

import (
	"time"

	"github.com/nostosx/kitx/backoff"

	"github.com/nostosx/servex/messaging/jobqueue"
)

const (
	defaultConcurrency = 4
	backoffBase        = 100 * time.Millisecond
	backoffCap         = 10 * time.Second
)

// Option configures a Queue.
type Option func(*options)

type options struct {
	concurrency int
	backoff     func(attempt int) time.Duration
	failed      func(job *jobqueue.Job, err error)
	now         func() time.Time
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
// It is called synchronously from the worker goroutine. By default such jobs
// are dropped.
func WithFailed(fn func(job *jobqueue.Job, err error)) Option {
	return func(o *options) { o.failed = fn }
}

// WithClock sets the clock used to schedule delayed jobs and retries and to
// expire unique keys; it is meant for tests. Timers still wait in real time
// (or bubble time under testing/synctest), so the clock should advance along
// with it. The default is time.Now; a nil now restores it.
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

// defaultBackoff doubles from backoffBase per attempt, capped at backoffCap.
var defaultBackoff = backoff.Exponential(backoffBase, backoffCap)
