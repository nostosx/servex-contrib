package messaging

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	"github.com/nostosx/servex/messaging"
)

// Bulkhead admits processing through an explicitly shared limiter. A nil limiter
// panics during assembly so a missing dependency cannot surface on a live message.
func Bulkhead(limiter *bulkhead.Limiter) messaging.ConsumeMiddleware {
	if limiter == nil {
		panic("bulkhead: nil limiter")
	}
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, msg *messaging.Message) error {
			release, err := limiter.Acquire(ctx)
			if err != nil {
				return err
			}
			defer release()
			return next(ctx, msg)
		}
	}
}
