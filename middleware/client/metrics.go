package client

import (
	"context"
	"time"

	"github.com/nostosx/servex-contrib/middleware/observe"
	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/transport"
)

// Metric names registered by Metrics.
const (
	RequestsTotal   = "servex_requests_total"
	RequestDuration = "servex_request_duration_seconds"
)

// MetricsOption configures duration histogram buckets.
type MetricsOption = observe.MetricsOption

// WithMetricBuckets copies explicit histogram bucket upper bounds in seconds.
func WithMetricBuckets(b []float64) MetricsOption { return observe.WithMetricBuckets(b) }

// Metrics returns middleware that records every call with m:
//
//   - RequestsTotal, a counter labelled kind, protocol, operation and code,
//     the errorx.Code name of the result ("OK" on success);
//   - RequestDuration, a histogram of call durations in seconds labelled kind,
//     protocol and operation.
//
// Label values come from the metadata.Info in the context and are empty when
// it is absent. The metrics are created once, when Metrics is called. A nil m
// uses noopmetrics.New().
func Metrics(m metrics.Meter, opts ...MetricsOption) endpoint.Middleware {
	if m == nil {
		m = noopmetrics.New()
	}
	buckets := observe.MetricBuckets(metrics.DefaultBuckets, opts...)
	requests := m.Counter(RequestsTotal,
		"Total number of calls, by kind, protocol, operation and result code.",
		"kind", "protocol", "operation", "code")
	durations := m.Histogram(RequestDuration,
		"Duration of calls in seconds, by kind, protocol and operation.",
		buckets, "kind", "protocol", "operation")
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (resp any, err error) {
			start := time.Now()
			finished := false
			defer func() {
				elapsed := time.Since(start).Seconds()
				outcome := observe.Outcome(err, finished)
				info, _ := metadata.FromContext(ctx)
				kind := string(info.Kind)
				requests.With(kind, info.Protocol, info.Operation, outcome.Code.String()).Add(ctx, 1)
				durations.With(kind, info.Protocol, info.Operation).Observe(ctx, elapsed)
			}()
			resp, err = next(ctx, req)
			finished = true
			return resp, err
		}
	}
}

// AttemptMetrics observes one selected-node invocation without replaying it.
func AttemptMetrics(meter metrics.Meter, opts ...MetricsOption) transport.AttemptMiddleware {
	begin := observe.Metrics("client_attempt", meter, observe.MetricBuckets(metrics.DefaultBuckets, opts...))
	return func(next transport.Invoker) transport.Invoker {
		return transport.InvokerFunc(func(ctx context.Context, address string, request any) (response any, err error) {
			info, _ := metadata.FromContext(ctx)

			attempt, _ := transport.AttemptFromContext(ctx)
			ctx, finish := begin(ctx, info.Operation, "attempt", attempt.Number, "peer", address)
			finished := false
			defer func() { finish(err, finished) }()

			response, err = next.Invoke(ctx, address, request)
			finished = true
			return response, err
		})
	}
}
