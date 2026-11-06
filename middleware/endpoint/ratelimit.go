package endpoint

import (
	"context"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// ReasonRateLimited is the errorx reason of errors returned for rejected
// calls.
const ReasonRateLimited = "RATE_LIMITED"

// RateLimiter decides whether a call identified by key may proceed. Allow must be
// safe for concurrent use.
type RateLimiter interface {
	Allow(key string) bool
}

// RateLimitOption configures the middleware returned by RateLimit.
type RateLimitOption func(*rateLimitOptions)

type rateLimitOptions struct {
	key      func(ctx context.Context, req any) string
	failOpen bool
}

// WithRateLimitKey sets the function deriving the limiter key of a call, for example
// the principal stored by the auth middleware or a client address. The
// default returns "", so all calls share one global limit. A nil fn keeps the
// default.
func WithRateLimitKey(fn func(ctx context.Context, req any) string) RateLimitOption {
	return func(o *rateLimitOptions) {
		if fn != nil {
			o.key = fn
		}
	}
}

// RateLimit returns middleware that asks l whether each call may proceed. A
// rejected call does not reach the next endpoint and returns
// errorx.ResourceExhausted with reason ReasonRateLimited, which is retryable.
func RateLimit(l RateLimiter, opts ...RateLimitOption) endpoint.Middleware {
	o := rateLimitOptions{key: func(context.Context, any) string { return "" }}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if !l.Allow(o.key(ctx, req)) {
				return nil, errorx.New(errorx.ResourceExhausted, ReasonRateLimited, "rate limit exceeded")
			}
			return next(ctx, req)
		}
	}
}
