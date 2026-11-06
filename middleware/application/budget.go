// Budget middleware applies cooperative application execution deadlines.
package application

import (
	"context"
	"time"

	"github.com/nostosx/servex/application"
)

// Budget bounds execution without extending an earlier caller deadline.
// A nonpositive duration leaves the handler unchanged.
func Budget[Request, Result any](duration time.Duration) application.Middleware[Request, Result] {
	return func(_ application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		if duration <= 0 {
			return next
		}
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			return next.Handle(ctx, request)
		})
	}
}
