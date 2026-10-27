package metrics

import (
	"context"
	"slices"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/metrics"
)

// Metric names registered by New.
const (
	RequestsTotal   = "servex_requests_total"
	RequestDuration = "servex_request_duration_seconds"
)

// Option configures the middleware returned by New.
type Option func(*options)

type options struct {
	buckets []float64
}

// WithBuckets sets the bucket upper bounds, in seconds, of the duration
// histogram. The default is metrics.DefaultBuckets. An empty slice keeps the
// default. The slice is copied.
func WithBuckets(b []float64) Option {
	return func(o *options) {
		if len(b) > 0 {
			o.buckets = slices.Clone(b)
		}
	}
}

// New returns middleware that records every call with m:
//
//   - RequestsTotal, a counter labelled kind, protocol, operation and code,
//     the errorx.Code name of the result ("OK" on success);
//   - RequestDuration, a histogram of call durations in seconds labelled kind,
//     protocol and operation.
//
// Label values come from the metadata.Info in the context and are empty when
// it is absent. The metrics are created once, when New is called. A nil m
// uses metrics.Noop().
func New(m metrics.Meter, opts ...Option) endpoint.Middleware {
	if m == nil {
		m = metrics.Noop()
	}
	o := options{buckets: metrics.DefaultBuckets}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	requests := m.Counter(RequestsTotal,
		"Total number of calls, by kind, protocol, operation and result code.",
		"kind", "protocol", "operation", "code")
	durations := m.Histogram(RequestDuration,
		"Duration of calls in seconds, by kind, protocol and operation.",
		o.buckets, "kind", "protocol", "operation")
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			elapsed := time.Since(start)

			info, _ := metadata.FromContext(ctx)
			kind := string(info.Kind)
			requests.With(kind, info.Protocol, info.Operation, errorx.CodeOf(err).String()).Add(ctx, 1)
			durations.With(kind, info.Protocol, info.Operation).Observe(ctx, elapsed.Seconds())
			return resp, err
		}
	}
}
