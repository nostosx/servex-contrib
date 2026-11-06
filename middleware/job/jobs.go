// Package job provides native enqueue and single-attempt execution policies.
// Queue implementations retain scheduling, leases and completion ownership.
package job

import (
	"context"
	"time"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
)

func Timeout(duration time.Duration) job.ExecutionMiddleware {
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, job *job.Job) error {
			if duration <= 0 {
				return next(ctx, job)
			}
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			return next(ctx, job)
		}
	}
}

func Recovery() job.ExecutionMiddleware {
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, job *job.Job) (err error) {
			defer func() {
				if recover() != nil {
					err = errorx.New(errorx.Internal, "JOB_PANIC", "job execution panicked")
				}
			}()
			return next(ctx, job)
		}
	}
}
