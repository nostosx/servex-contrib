// Package observe shares signal recording, not execution semantics or middleware
// contracts. Each public scope owns its native wrapper and propagation policy.
package observe

import (
	"context"
	"fmt"
	"slices"
	"time"

	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// Outcome never formats err or records localized messages, metadata or causes.
// Interruption is observed without recovering or replacing the control flow.
func Outcome(err error, finished bool) errorx.Classification {
	if !finished {
		return errorx.Classification{Code: errorx.Internal, Reason: "INTERRUPTED"}
	}
	return errorx.Classify(err)
}

// Level chooses a completion-log severity from a bounded canonical code.
func Level(code errorx.Code) logger.Level {
	if code == errorx.OK {
		return logger.LevelInfo
	}
	if errorx.IsServerFault(code) {
		return logger.LevelError
	}
	return logger.LevelWarn
}

// Fields returns owned stable error fields and the current trace correlation.
// Reason and trace IDs are diagnostic fields, never default metric labels.
func Fields(ctx context.Context, c errorx.Classification) []any {
	fields := []any{"code", c.Code.String()}
	if c.Code != errorx.OK {
		if c.Reason != "" {
			fields = append(fields, "reason", c.Reason)
		}
		fields = append(fields, "retryable", c.Retryable, "remote", c.Remote)
	}
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		traceID, spanID := span.TraceID(), span.SpanID()
		if traceID != "" && spanID != "" {
			fields = append(fields, "trace_id", traceID, "span_id", spanID)
		}
	}
	return fields
}

// Record adds payload-free classification. The caller owns failed's meaning:
// server/business spans distinguish expected rejections, while network and
// message/job attempts report unsuccessful processing as failed attempts.
func Record(span tracing.Span, c errorx.Classification, failed bool) {
	span.SetAttribute("servex.code", c.Code.String())
	if c.Code == errorx.OK {
		return
	}
	if c.Reason != "" {
		span.SetAttribute("servex.reason", c.Reason)
	}
	span.SetAttribute("servex.retryable", c.Retryable)
	span.SetAttribute("servex.remote", c.Remote)
	if failed {
		safe := errorx.New(c.Code, c.Reason, c.Code.String()).WithRetryable(c.Retryable)
		if c.Remote {
			safe = safe.WithRemote()
		}
		span.RecordError(safe)
	}
}

// Begin records one signal around a native execution scope. It never invokes work.
type Begin func(context.Context, string, ...any) (context.Context, func(error, bool))

// Logging records safe completion fields without starting or replacing a span.
func Logging(name string, log logger.Logger, level func(errorx.Code) logger.Level) Begin {
	return func(ctx context.Context, operation string, attrs ...any) (context.Context, func(error, bool)) {
		start := time.Now()
		return ctx, func(err error, finished bool) {
			elapsed := time.Since(start)
			outcome := Outcome(err, finished)
			severity := level(outcome.Code)
			if log == nil || !log.Enabled(ctx, severity) {
				return
			}
			fields := []any{"scope", name, "operation", operation, "duration", elapsed}
			fields = append(fields, Fields(ctx, outcome)...)
			fields = append(fields, attrs...)
			log.Log(ctx, severity, name, fields...)
		}
	}
}

// Metrics creates bounded instruments once and records only their own duration.
func Metrics(name string, meter metrics.Meter, buckets []float64, labels ...string) Begin {
	if meter == nil {
		meter = noopmetrics.New()
	}
	labels = slices.Clone(labels)
	calls := meter.Counter("servex_"+name+"_total", "Scoped execution outcomes.", append(append([]string{"operation"}, labels...), "code")...)
	duration := meter.Histogram("servex_"+name+"_duration_seconds", "Scoped execution duration.", slices.Clone(buckets), append([]string{"operation"}, labels...)...)
	return func(ctx context.Context, operation string, attrs ...any) (context.Context, func(error, bool)) {
		start := time.Now()
		return ctx, func(err error, finished bool) {
			elapsed := time.Since(start)
			outcome := Outcome(err, finished)
			values := []string{operation}
			for _, label := range labels {
				value := ""
				for i := 0; i+1 < len(attrs); i += 2 {
					if attrs[i] == label {
						value = fmt.Sprint(attrs[i+1])
						break
					}
				}
				values = append(values, value)
			}
			calls.With(append(slices.Clone(values), outcome.Code.String())...).Add(ctx, 1)
			duration.With(values...).Observe(ctx, elapsed.Seconds())
		}
	}
}

// Tracing records only a span. A nil tracer leaves the current context intact.
func Tracing(name string, tracer tracing.Tracer, kind tracing.SpanKind) Begin {
	return func(ctx context.Context, operation string, attrs ...any) (context.Context, func(error, bool)) {
		if tracer == nil {
			return ctx, func(error, bool) {}
		}
		ctx, span := tracer.Start(ctx, operation, kind)
		ctx = tracing.ContextWithSpan(ctx, span)
		return ctx, func(err error, finished bool) {
			defer span.End()
			outcome := Outcome(err, finished)
			span.SetAttribute("servex.scope", name)
			for i := 0; i+1 < len(attrs); i += 2 {
				span.SetAttribute(attrs[i].(string), attrs[i+1])
			}
			Record(span, outcome, outcome.Code != errorx.OK)
		}
	}
}
