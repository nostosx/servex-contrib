package job

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	"github.com/nostosx/servex/job"
)

// Bulkhead admits processing through an explicitly shared limiter. A nil limiter
// panics during assembly so a missing dependency cannot surface on a live job.
func Bulkhead(limiter *bulkhead.Limiter) job.ExecutionMiddleware {
	if limiter == nil {
		panic("bulkhead: nil limiter")
	}
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, task *job.Job) error {
			release, err := limiter.Acquire(ctx)
			if err != nil {
				return err
			}
			defer release()
			return next(ctx, task)
		}
	}
}
