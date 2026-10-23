// Package retry provides client middleware that calls the next endpoint
// again after retryable failures, waiting with a backoff between attempts.
//
// Retrying stops as soon as the caller's context ends, and errors produced by
// an open circuit breaker are never retried. In the recommended client chain
// retry wraps circuitbreaker and timeout, and node selection is the innermost
// step, so each attempt may reach a different node.
package retry
