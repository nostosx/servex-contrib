package job

import (
	"context"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/storage/idempotency"
)

// Deduplicate records execution success with participating SQL writes. Its key
// callback supplies trusted scope and a canonical digest excluding Attempt;
// stable Job.ID is the token. Queue completion remains outside this transaction.
func Deduplicate(store idempotency.Store, key func(context.Context, *job.Job) (idempotency.Key, error)) job.ExecutionMiddleware {
	if store == nil || key == nil {
		panic("jobs: nil idempotency dependency")
	}
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, job *job.Job) error {
			if job == nil || job.ID == "" {
				return errorx.New(errorx.InvalidArgument, "JOB_ID_REQUIRED", "stable job id required")
			}
			identity, err := key(ctx, job)
			if err != nil {
				return err
			}
			identity.Token = job.ID
			identity.Operation = "execute:" + job.Type
			_, err = store.Do(ctx, identity, func(txctx context.Context) ([]byte, error) { return nil, next(txctx, job) })
			return err
		}
	}
}
