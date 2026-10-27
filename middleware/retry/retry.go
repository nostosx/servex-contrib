package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"

	"github.com/nostosx/kitx/backoff"
)

// Defaults used by New.
const (
	// DefaultMaxAttempts is the default total number of attempts.
	DefaultMaxAttempts = 3
	// DefaultBaseDelay is the delay before the first retry of the default
	// backoff, before jitter.
	DefaultBaseDelay = 100 * time.Millisecond
	// DefaultMaxDelay caps the delays of the default backoff.
	DefaultMaxDelay = 2 * time.Second
)

// Option configures the middleware returned by New.
type Option func(*options)

type options struct {
	maxAttempts int
	backoff     func(attempt int) time.Duration
	retryIf     func(error) bool
}

// WithMaxAttempts sets the total number of attempts, including the first
// one. The default is DefaultMaxAttempts; values below 1 are treated as 1,
// which disables retrying.
func WithMaxAttempts(n int) Option {
	return func(o *options) { o.maxAttempts = max(n, 1) }
}

// WithBackoff sets the function returning how long to wait before the next
// attempt, given the number of attempts made so far (1 before the first
// retry). A non-positive delay retries immediately. The default is
// Exponential(DefaultBaseDelay, DefaultMaxDelay); nil keeps the default.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(o *options) {
		if fn != nil {
			o.backoff = fn
		}
	}
}

// WithRetryIf sets the predicate deciding whether a failed attempt may be
// retried. The default is errorx.IsRetryable, which also retries remote
// errors received by this hop (see the package documentation); nil keeps the
// default. Errors of an open circuit breaker are never retried, whatever fn
// returns.
func WithRetryIf(fn func(error) bool) Option {
	return func(o *options) {
		if fn != nil {
			o.retryIf = fn
		}
	}
}

// Exponential returns a backoff that doubles from base for every attempt, is
// capped at limit, and applies equal jitter: the delay for attempt n is
// uniformly distributed in [d/2, d] where d = min(base*2^(n-1), limit).
// It uses math/rand/v2 and is safe for concurrent use.
func Exponential(base, limit time.Duration) func(attempt int) time.Duration {
	delay := backoff.Exponential(base, limit)
	return func(attempt int) time.Duration {
		d := delay(attempt)
		if d <= 0 {
			return 0
		}
		half := d / 2
		return half + rand.N(d-half+1)
	}
}

// New returns middleware that retries the next endpoint.
//
// An attempt that fails is retried when the retry predicate accepts its error,
// the attempt limit has not been reached and the caller's context is still
// active; between attempts the middleware waits for the backoff delay. The
// call returns the response and error of the last attempt. When the caller's
// context ends, no further attempt is made and the error of the last attempt
// is returned at once, including while waiting for the backoff.
//
// Errors matching circuitbreaker.ErrOpen are never retried, so an open
// breaker inside the retry middleware fails the call immediately.
func New(opts ...Option) endpoint.Middleware {
	o := options{
		maxAttempts: DefaultMaxAttempts,
		backoff:     Exponential(DefaultBaseDelay, DefaultMaxDelay),
		retryIf:     errorx.IsRetryable,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			for attempt := 1; ; attempt++ {
				resp, err := next(ctx, req)
				if err == nil || attempt >= o.maxAttempts || ctx.Err() != nil || !o.retryable(err) {
					return resp, err
				}
				if !wait(ctx, o.backoff(attempt)) {
					return resp, err
				}
			}
		}
	}
}

// retryable reports whether err may be retried.
func (o *options) retryable(err error) bool {
	return !errors.Is(err, circuitbreaker.ErrOpen) && o.retryIf(err)
}

// wait blocks for d or until ctx ends, reporting whether the full delay
// elapsed with ctx still active.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}
