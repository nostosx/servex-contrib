package job

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/observability/metrics"
)

// MetricsOption configures duration histogram buckets.
type MetricsOption = observe.MetricsOption

// WithMetricBuckets copies explicit histogram bucket upper bounds in seconds.
func WithMetricBuckets(b []float64) MetricsOption { return observe.WithMetricBuckets(b) }

// EnqueueMetrics observes native enqueue results and preserves all queue options.
func EnqueueMetrics(meter metrics.Meter, opts ...MetricsOption) job.EnqueueMiddleware {
	begin := metricBegin("job_enqueue", meter, nil, opts...)
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

// ExecutionMetrics observes one job execution attempt without installing recovery or retries.
func ExecutionMetrics(meter metrics.Meter, opts ...MetricsOption) job.ExecutionMiddleware {
	begin := metricBegin("job_attempt", meter, nil, opts...)
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

// EventMetrics observes native runtime events, distinguishing attempts from final completion.
func EventMetrics(meter metrics.Meter, opts ...MetricsOption) job.Observer {
	begin := metricBegin("job_runtime", meter, []string{"event"}, opts...)
	return func(ctx context.Context, event job.Event) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.JobType, "event", event.Kind, "queue", event.Queue,
			"job_id", event.JobID, "attempt", event.Attempt,
			"handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}

func metricBegin(name string, meter metrics.Meter, labels []string, opts ...MetricsOption) observe.Begin {
	return observe.Metrics(name, meter, observe.MetricBuckets(metrics.DefaultBuckets, opts...), labels...)
}
