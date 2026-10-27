// Package retry provides client middleware that calls the next endpoint
// again after retryable failures, waiting with a backoff between attempts.
//
// Retrying stops as soon as the caller's context ends, and errors produced by
// an open circuit breaker are never retried. In the recommended client chain
// retry wraps circuitbreaker and timeout, and node selection is the innermost
// step, so each attempt may reach a different node.
//
// The default predicate, errorx.IsRetryable, retries an error the calling
// service received from its peer. Transport clients mark such errors remote
// (errorx.WithRemote), and servex servers report them to their own callers
// as not retryable (errorx.PropagatedRetryable), so a failure deep in a call
// chain is retried only by the hop that received it instead of at every hop.
// A handler that wants its caller to retry a downstream failure sets the
// decision explicitly with WithRetryable(true).
package retry
