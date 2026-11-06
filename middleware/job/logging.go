package job

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/observability/logger"
)

// Shared log field names.
const (
	Message      = "call"
	KeyKind      = "kind"
	KeyProtocol  = "protocol"
	KeyOperation = "operation"
	KeyDuration  = "duration"
	KeyCode      = "code"
	KeyReason    = "reason"
	KeyRetryable = "retryable"
	KeyRemote    = "remote"
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
)

// LoggingOption configures log levels for this execution scope.
type LoggingOption = observe.LoggingOption

// WithLogLevel chooses a log level from the classified outcome.
func WithLogLevel(fn func(errorx.Code) logger.Level) LoggingOption { return observe.WithLogLevel(fn) }

// EnqueueLogging observes native enqueue results and preserves all queue options.
func EnqueueLogging(log logger.Logger, opts ...LoggingOption) job.EnqueueMiddleware {
	begin := logBegin("job_enqueue", log, opts...)
	return func(next job.EnqueueFunc) job.EnqueueFunc {
		return func(ctx context.Context, task *job.Job, options ...job.EnqueueOption) (id string, err error) {
			if task == nil {
				return "", errorx.New(errorx.InvalidArgument, "JOB_REQUIRED", "job required")
			}

			ctx, finish := begin(ctx, task.Type)
			finished := false
			defer func() { finish(err, finished) }()

			id, err = next(ctx, task, options...)
			finished = true
			return id, err
		}
	}
}

// ExecutionLogging observes one job execution attempt without installing recovery or retries.
func ExecutionLogging(log logger.Logger, opts ...LoggingOption) job.ExecutionMiddleware {
	begin := logBegin("job_attempt", log, opts...)
	return func(next job.Handler) job.Handler {
		return func(ctx context.Context, task *job.Job) (err error) {
			if task == nil {
				return errorx.New(errorx.InvalidArgument, "JOB_REQUIRED", "job required")
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

// EventLogging observes native runtime events, distinguishing attempts from final completion.
func EventLogging(log logger.Logger, opts ...LoggingOption) job.Observer {
	begin := logBegin("job_runtime", log, opts...)
	return func(ctx context.Context, event job.Event) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.JobType, "event", event.Kind, "queue", event.Queue,
			"job_id", event.JobID, "attempt", event.Attempt,
			"handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}

func logBegin(name string, log logger.Logger, opts ...LoggingOption) observe.Begin {
	return observe.Logging(name, log, observe.LogLevel(opts...))
}
