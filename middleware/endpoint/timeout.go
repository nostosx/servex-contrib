package endpoint

import (
	"context"
	"time"

	"github.com/nostosx/servex/endpoint"
)

// Timeout bounds the context passed to one call without spawning a goroutine.
// Nonpositive durations keep the caller context. It never retries; handlers must
// honor cancellation. A canceled call cannot report a successful response.
func Timeout(duration time.Duration) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			if duration <= 0 {
				return next(ctx, request)
			}
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			response, err := next(ctx, request)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return response, err
		}
	}
}
