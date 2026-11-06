package observe_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/observe"
	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

type signals struct {
	tracing.Span
	level   logger.Level
	fields  []any
	failure error
	ended   int
	values  []string
	labels  []string
	elapsed float64
}

func (s *signals) Start(ctx context.Context, _ string, _ tracing.SpanKind) (context.Context, tracing.Span) {
	return ctx, s
}
func (*signals) SetAttribute(string, any)                   {}
func (s *signals) RecordError(err error)                    { s.failure = err; time.Sleep(time.Second) }
func (s *signals) End()                                     { s.ended++ }
func (*signals) TraceID() string                            { return "trace" }
func (*signals) SpanID() string                             { return "span" }
func (*signals) Enabled(context.Context, logger.Level) bool { return true }
func (s *signals) Log(_ context.Context, level logger.Level, _ string, fields ...any) {
	s.level, s.fields = level, fields
}
func (s *signals) Counter(_ string, _ string, labels ...string) metrics.Counter {
	s.labels = labels
	return count{s}
}
func (*signals) Gauge(name, help string, labels ...string) metrics.Gauge {
	return noopmetrics.New().Gauge(name, help, labels...)
}
func (s *signals) Histogram(string, string, []float64, ...string) metrics.Histogram {
	return duration{s}
}

type count struct{ s *signals }

func (c count) With(values ...string) metrics.Counter { c.s.values = values; return c }
func (count) Add(context.Context, float64)            {}

type duration struct{ s *signals }

func (d duration) With(...string) metrics.Histogram           { return d }
func (d duration) Observe(_ context.Context, elapsed float64) { d.s.elapsed = elapsed }

func TestNativeSignalsPreserveClassificationAndExcludeSinkTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, code := range []errorx.Code{errorx.PermissionDenied, errorx.Unavailable, errorx.Code(99), errorx.OK} {
			s := &signals{}
			failure := errorx.New(code, "POLICY", "private localized text").WithMetadata(map[string]string{"secret": "private metadata"}).WithRemote()
			ctx, finishTrace := observe.Tracing("client_attempt", s, tracing.SpanKindClient)(t.Context(), "catalog.get", "peer", "node:80")
			_, finishLog := observe.Logging("client_attempt", s, observe.Level)(ctx, "catalog.get", "peer", "node:80")
			_, finishMetrics := observe.Metrics("client_attempt", s, metrics.DefaultBuckets)(ctx, "catalog.get", "peer", "node:80")
			if tracing.SpanFromContext(ctx) != s {
				t.Fatal("current span not bound")
			}
			time.Sleep(200 * time.Millisecond)
			finishMetrics(failure, true)
			finishLog(failure, true)
			finishTrace(failure, true)
			want := errorx.Classify(failure)
			if s.ended != 1 || errorx.Classify(s.failure) != want || strings.Contains(fmt.Sprint(s.fields, s.failure), "private") {
				t.Fatalf("unsafe or incomplete recording: fields=%v failure=%v", s.fields, s.failure)
			}
			expectedLevel := logger.LevelError
			if code == errorx.PermissionDenied {
				expectedLevel = logger.LevelWarn
			}
			if s.level != expectedLevel || s.elapsed != .2 {
				t.Fatalf("level=%v duration=%v", s.level, s.elapsed)
			}
			if fmt.Sprint(s.labels) != "[operation code]" || fmt.Sprint(s.values) != "[catalog.get "+want.Code.String()+"]" {
				t.Fatalf("metric dimensions leaked: %v %v", s.labels, s.values)
			}
			fields := map[string]any{}
			for i := 0; i < len(s.fields); i += 2 {
				fields[s.fields[i].(string)] = s.fields[i+1]
			}
			if fields["reason"] != "POLICY" || fields["trace_id"] != "trace" || fields["span_id"] != "span" {
				t.Fatalf("missing correlation: %v", fields)
			}
		}
	})
}

func TestDisabledSignalsPreserveCallerSpan(t *testing.T) {
	parent := &signals{}
	ctx := tracing.ContextWithSpan(t.Context(), parent)
	for _, begin := range []observe.Begin{
		observe.Logging("disabled", nil, observe.Level),
		observe.Metrics("disabled", nil, metrics.DefaultBuckets),
		observe.Tracing("disabled", nil, tracing.SpanKindClient),
	} {
		got, finish := begin(ctx, "catalog.get")
		if got != ctx || tracing.SpanFromContext(got) != parent {
			t.Fatal("disabled signal replaced caller context or span")
		}
		finish(nil, true)
	}
	if parent.ended != 0 || parent.failure != nil {
		t.Fatal("disabled signal modified caller span")
	}
}
