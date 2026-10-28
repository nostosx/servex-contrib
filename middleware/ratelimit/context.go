package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// ContextLimiter supports remote limits. A positive retryAfter is a hint
// for a rejected call; err represents a backend failure, not quota rejection.
// The structural contract lets adapters implement it without importing this
// module. Implementations must honor cancellation and be concurrency-safe.
type ContextLimiter interface {
	AllowContext(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}

// ReasonBackendUnavailable identifies a failure to consult the limit backend.
const ReasonBackendUnavailable = "RATE_LIMIT_BACKEND_UNAVAILABLE"

// RetryAfterMetadataKey holds the retry hint in integer milliseconds.
const RetryAfterMetadataKey = "retry_after_ms"

// WithFailOpen makes NewContext proceed when its backend fails. It never
// ignores caller cancellation or a normal quota rejection. By default a
// backend error fails the call. This option has no effect on New.
func WithFailOpen(enabled bool) Option { return func(o *options) { o.failOpen = enabled } }

// NewContext consults a remote limiter with the caller's context. It retains
// errorx backend errors and wraps other failures as Unavailable. The caller
// should set a deadline; this middleware does not add one or retry the check.
// Backend-owned timeouts follow the failure policy when the caller is still
// active; only cancellation of the caller's context always stops the call.
func NewContext(l ContextLimiter, opts ...Option) endpoint.Middleware {
	o := options{key: func(context.Context, any) string { return "" }}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			allowed, delay, err := l.AllowContext(ctx, o.key(ctx, req))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil {
				if o.failOpen {
					return next(ctx, req)
				}
				if _, ok := errors.AsType[*errorx.Error](err); ok {
					return nil, err
				}
				return nil, errorx.New(errorx.Unavailable, ReasonBackendUnavailable, "rate limit backend unavailable").WithCause(err)
			}
			if !allowed {
				e := errorx.New(errorx.ResourceExhausted, ReasonRateLimited, "rate limit exceeded")
				if delay > 0 {
					ms := delay / time.Millisecond
					if delay%time.Millisecond != 0 {
						ms++
					}
					e = e.WithMetadata(map[string]string{RetryAfterMetadataKey: strconv.FormatInt(int64(ms), 10)})
				}
				return nil, e
			}
			return next(ctx, req)
		}
	}
}
