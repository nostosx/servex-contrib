package timeout

import (
	"context"
	"errors"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// ReasonTimeout is the errorx reason of errors produced when the attempt
// deadline set by New is exceeded.
const ReasonTimeout = "TIMEOUT"

// errExpired is the context cause recorded when the deadline set by New
// expires. It distinguishes that deadline from one inherited from the
// caller.
var errExpired = errors.New("timeout: attempt deadline exceeded")

// New returns middleware that runs the next endpoint with a context whose
// deadline is at most d from now. A non-positive d disables the middleware.
//
// When the deadline set by New expires and the next endpoint returns an
// error, the call fails with an errorx.DeadlineExceeded error with reason
// ReasonTimeout that keeps the original error as its cause. It is retryable
// by default, so an outer retry may try again while the caller's own deadline
// allows it. Errors returned before the deadline, and errors caused by an
// earlier deadline or cancellation of the caller's context, are returned
// unchanged, as is a successful response that arrives late.
//
// The next endpoint runs on the calling goroutine and must observe its
// context for the deadline to interrupt it.
func New(d time.Duration) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		if d <= 0 {
			return next
		}
		return func(ctx context.Context, req any) (any, error) {
			ctx, cancel := context.WithTimeoutCause(ctx, d, errExpired)
			defer cancel()
			resp, err := next(ctx, req)
			if err != nil && ctx.Err() != nil && errors.Is(context.Cause(ctx), errExpired) {
				return resp, errorx.Newf(errorx.DeadlineExceeded, ReasonTimeout,
					"timeout: attempt exceeded %v", d).WithCause(err)
			}
			return resp, err
		}
	}
}
