// Bulkhead middleware admits typed business calls through an explicitly shared limiter.
package application

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	"github.com/nostosx/servex/application"
)

// Bulkhead shares the supplied admission limit across decorated calls.
func Bulkhead[Request, Result any](limiter *bulkhead.Limiter) application.Middleware[Request, Result] {
	if limiter == nil {
		panic("bulkhead: nil limiter")
	}
	return func(_ application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			release, err := limiter.Acquire(ctx)
			if err != nil {
				var zero Result
				return zero, err
			}
			defer release()
			return next.Handle(ctx, request)
		})
	}
}
