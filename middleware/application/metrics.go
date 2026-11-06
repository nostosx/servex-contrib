package application

import (
	"context"
	"slices"
	"time"

	"github.com/nostosx/servex-contrib/middleware/observe"
	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/observability/metrics"
)

// Metric names for this execution scope.
const (
	UsecaseCallsTotal = "servex_usecase_calls_total"
	UsecaseDuration   = "servex_usecase_duration_seconds"
)

// MetricsOption configures duration histogram buckets.
type MetricsOption = observe.MetricsOption

// WithMetricBuckets copies explicit histogram bucket upper bounds in seconds.
func WithMetricBuckets(b []float64) MetricsOption { return observe.WithMetricBuckets(b) }

// Metrics observes one typed business invocation without changing its result or error.
func Metrics[Request, Result any](meter metrics.Meter, opts ...MetricsOption) application.Middleware[Request, Result] {
	if meter == nil {
		meter = noopmetrics.New()
	}
	buckets := observe.MetricBuckets(slices.Clone(metrics.DefaultBuckets), opts...)
	calls := meter.Counter(UsecaseCallsTotal, "Total application calls by kind, operation and result code.", "kind", "operation", "code")
	durations := meter.Histogram(UsecaseDuration, "Application call durations in seconds by kind and operation.", buckets, "kind", "operation")
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, req Request) (Result, error) {
			start := time.Now()
			finished := false
			var failure error
			defer func() {
				elapsed := time.Since(start).Seconds()
				calls.With(string(meta.Kind), meta.Operation, observe.Outcome(failure, finished).Code.String()).Add(ctx, 1)
				durations.With(string(meta.Kind), meta.Operation).Observe(ctx, elapsed)
			}()
			result, err := next.Handle(ctx, req)
			failure, finished = err, true
			return result, err
		})
	}
}
