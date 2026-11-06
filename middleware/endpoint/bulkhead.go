// Bulkhead middleware admits inbound calls through an explicitly shared limiter.
package endpoint

import (
	"context"

	"github.com/nostosx/servex/endpoint"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
)

// Bulkhead shares the supplied admission limit across decorated calls. A nil
// limiter panics during assembly.
func Bulkhead(limiter *bulkhead.Limiter) endpoint.Middleware {
	if limiter == nil {
		panic("bulkhead: nil limiter")
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			release, err := limiter.Acquire(ctx)
			if err != nil {
				return nil, err
			}
			defer release()
			return next(ctx, request)
		}
	}
}
