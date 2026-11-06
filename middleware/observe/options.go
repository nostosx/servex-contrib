package observe

import (
	"slices"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
)

// LoggingOption configures level selection shared by execution boundaries.
type LoggingOption func(*loggingOptions)

type loggingOptions struct {
	level func(errorx.Code) logger.Level
}

// WithLogLevel sets completion severity from a canonical code. Nil keeps the default.
func WithLogLevel(fn func(errorx.Code) logger.Level) LoggingOption {
	return func(o *loggingOptions) {
		if fn != nil {
			o.level = fn
		}
	}
}

// LogLevel resolves options during assembly. The returned function uses Level
// unless overridden and must be called even when log output is disabled.
func LogLevel(opts ...LoggingOption) func(errorx.Code) logger.Level {
	o := loggingOptions{level: Level}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.level
}

// MetricsOption configures histogram buckets shared by execution boundaries.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	buckets []float64
}

// WithMetricBuckets copies non-empty bucket bounds when options are applied.
// Empty bounds retain the supplied defaults.
func WithMetricBuckets(bounds []float64) MetricsOption {
	return func(o *metricsOptions) {
		if len(bounds) > 0 {
			o.buckets = slices.Clone(bounds)
		}
	}
}

// MetricBuckets resolves options during assembly. Defaults are returned unchanged
// unless replaced; callers choose whether to clone their default slice.
func MetricBuckets(defaults []float64, opts ...MetricsOption) []float64 {
	o := metricsOptions{buckets: defaults}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.buckets
}
