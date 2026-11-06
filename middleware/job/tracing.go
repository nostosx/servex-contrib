package job

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

// Span attribute names used by this execution scope.
const (
	AttrProtocol  = "servex.protocol"
	AttrOperation = "servex.operation"
	AttrCode      = "servex.code"
	AttrReason    = "servex.reason"
	AttrRetryable = "servex.retryable"
	AttrRemote    = "servex.remote"
)

// EnqueueTracing observes native enqueue results and preserves all queue options.
func EnqueueTracing(tracer tracing.Tracer, propagation tracing.Propagator) job.EnqueueMiddleware {
	begin := observe.Tracing("job_enqueue", tracer, tracing.SpanKindProducer)
	return func(next job.EnqueueFunc) job.EnqueueFunc {
		return func(ctx context.Context, task *job.Job, options ...job.EnqueueOption) (id string, err error) {
			if task == nil {
				return "", errorx.New(errorx.InvalidArgument, "JOB_REQUIRED", "job required")
			}
			if tracer != nil {
				copy := *task
				copy.Header = task.Header.Clone()
				copy.Payload = append([]byte(nil), task.Payload...)
				if copy.Header == nil {
					copy.Header = make(metadata.MD)
				}
				task = &copy
			}
			ctx, finish := begin(ctx, task.Type)
			finished := false
			defer func() { finish(err, finished) }()
			if tracer != nil && propagation != nil {
				propagation.Inject(ctx, task.Header)
			}
			id, err = next(ctx, task, options...)
			finished = true
			return id, err
		}
	}
}

// ExecutionTracing observes one job execution attempt without installing recovery or retries.
func ExecutionTracing(tracer tracing.Tracer, propagation tracing.Propagator) job.ExecutionMiddleware {
	begin := observe.Tracing("job_attempt", tracer, tracing.SpanKindConsumer)
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, task *job.Job) (err error) {
			if task == nil {
				return errorx.New(errorx.InvalidArgument, "JOB_REQUIRED", "job required")
			}
			if tracer != nil && propagation != nil {
				ctx = propagation.Extract(ctx, task.Header)
			}
			ctx, finish := begin(ctx, task.Type, "job_id", task.ID, "attempt", task.Attempt)
			finished := false
			defer func() { finish(err, finished) }()
			err = next(ctx, task)
			finished = true
			return err
		}
	}
}

// EventTracing observes native runtime events, distinguishing attempts from final completion.
func EventTracing(tracer tracing.Tracer) job.Observer {
	begin := observe.Tracing("job_runtime", tracer, tracing.SpanKindInternal)
	return func(ctx context.Context, event job.Event) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.JobType, "event", event.Kind, "queue", event.Queue,
			"job_id", event.JobID, "attempt", event.Attempt,
			"handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}
