package endpoint

import (
	"context"
	"time"

	"github.com/nostosx/servex/endpoint"
)

// Budget bounds execution without extending an earlier caller deadline.
// A nonpositive duration leaves the handler unchanged.
func Budget(duration time.Duration) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		if duration <= 0 {
			return next
		}
		return func(ctx context.Context, request any) (any, error) {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			return next(ctx, request)
		}
	}
}
