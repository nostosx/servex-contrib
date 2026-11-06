package observe_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	jobmw "github.com/nostosx/servex-contrib/middleware/job"
	messagingmw "github.com/nostosx/servex-contrib/middleware/messaging"

	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

type eventRecording struct {
	metrics.Meter
	labels  []string
	values  []string
	attrs   map[string]any
	fields  []any
	failure error
	level   logger.Level
	kind    tracing.SpanKind
	ended   int
	count   float64
}

func (r *eventRecording) Start(ctx context.Context, _ string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	r.kind = kind
	return ctx, r
}
func (r *eventRecording) SetAttribute(key string, value any)       { r.attrs[key] = value }
func (r *eventRecording) RecordError(err error)                    { r.failure = err }
func (r *eventRecording) End()                                     { r.ended++ }
func (*eventRecording) TraceID() string                            { return "trace" }
func (*eventRecording) SpanID() string                             { return "span" }
func (*eventRecording) Enabled(context.Context, logger.Level) bool { return true }
func (r *eventRecording) Log(_ context.Context, level logger.Level, _ string, fields ...any) {
	r.level, r.fields = level, fields
}
func (r *eventRecording) Counter(_, _ string, labels ...string) metrics.Counter {
	r.labels = append([]string(nil), labels...)
	return eventCounter{r}
}

type eventCounter struct{ recording *eventRecording }

func (c eventCounter) With(values ...string) metrics.Counter {
	c.recording.values = append([]string(nil), values...)
	return c
}
func (c eventCounter) Add(_ context.Context, delta float64) { c.recording.count += delta }

func TestSettlementsPreserveNativeOutcome(t *testing.T) {
	handlerErr := errorx.New(errorx.PermissionDenied, "BUSINESS_DENIED", "private handler payload")
	nativeErr := errorx.New(errorx.Unavailable, "ACK_FAILED", "private ack payload").WithRetryable(true).WithRemote()
	for _, tt := range []struct {
		name   string
		native error
		level  logger.Level
	}{
		{name: "processing error stays diagnostic", level: logger.LevelInfo},
		{name: "settlement failure controls outcome", native: nativeErr, level: logger.LevelError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
			observers := []messaging.SettlementObserver{messagingmw.SettlementLogging(r), messagingmw.SettlementMetrics(r), messagingmw.SettlementTracing(r)}
			event := messaging.Settlement{
				Topic: "orders", Group: "billing", MessageID: "message-42", Action: "ack",
				HandlerError: handlerErr, Error: tt.native,
			}
			for _, observer := range observers {
				observer(t.Context(), event)
			}
			want := errorx.Classify(tt.native)
			if r.ended != 1 || r.kind != tracing.SpanKindInternal || r.level != tt.level || r.count != 1 {
				t.Fatalf("incomplete native observation: ended=%d kind=%v level=%v count=%v", r.ended, r.kind, r.level, r.count)
			}
			if errorx.Classify(r.failure) != want || r.attrs["servex.code"] != want.Code.String() {
				t.Fatalf("processing error replaced settlement result: failure=%v attrs=%v", r.failure, r.attrs)
			}
			if r.attrs["handler_code"] != errorx.PermissionDenied.String() || r.attrs["handler_reason"] != "BUSINESS_DENIED" || r.attrs["action"] != "ack" || r.attrs["message_id"] != event.MessageID {
				t.Fatalf("missing native diagnostics: %v", r.attrs)
			}
			if !slices.Equal(r.labels, []string{"operation", "action", "code"}) || !slices.Equal(r.values, []string{"orders", "ack", want.Code.String()}) {
				t.Fatalf("unexpected metric dimensions: labels=%v values=%v", r.labels, r.values)
			}
			if strings.Contains(fmt.Sprint(r.failure, r.fields, r.attrs), "private") {
				t.Fatal("observation leaked native or handler error payload")
			}
		})
	}
	messagingmw.SettlementLogging(nil)(t.Context(), messaging.Settlement{Topic: "orders", Action: "ack"})
	messagingmw.SettlementMetrics(nil)(t.Context(), messaging.Settlement{Topic: "orders", Action: "ack"})
	messagingmw.SettlementTracing(nil)(t.Context(), messaging.Settlement{Topic: "orders", Action: "ack"})
}

func TestEventsPreserveRuntimeTransitionAndOutcome(t *testing.T) {
	handlerErr := errorx.New(errorx.PermissionDenied, "BUSINESS_DENIED", "private handler payload")
	nativeErr := errorx.New(errorx.Unavailable, "LEASE_LOST", "private runtime payload").WithRetryable(true).WithRemote()
	for _, tt := range []struct {
		name   string
		kind   string
		native error
		level  logger.Level
	}{
		{name: "attempt end stays distinct from completion", kind: "attempt_finished", level: logger.LevelInfo},
		{name: "runtime failure controls outcome", kind: "lease_lost", native: nativeErr, level: logger.LevelError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
			observers := []job.Observer{jobmw.EventLogging(r), jobmw.EventMetrics(r), jobmw.EventTracing(r)}
			event := job.Event{
				Kind: tt.kind, Queue: "critical", JobType: "refresh", JobID: "job-42", Attempt: 3,
				HandlerError: handlerErr, Error: tt.native,
			}
			for _, observer := range observers {
				observer(t.Context(), event)
			}
			want := errorx.Classify(tt.native)
			if r.ended != 1 || r.kind != tracing.SpanKindInternal || r.level != tt.level || r.count != 1 {
				t.Fatalf("incomplete runtime observation: ended=%d kind=%v level=%v count=%v", r.ended, r.kind, r.level, r.count)
			}
			if errorx.Classify(r.failure) != want || r.attrs["servex.code"] != want.Code.String() {
				t.Fatalf("handler error replaced runtime result: failure=%v attrs=%v", r.failure, r.attrs)
			}
			if r.attrs["handler_code"] != errorx.PermissionDenied.String() || r.attrs["handler_reason"] != "BUSINESS_DENIED" || r.attrs["event"] != tt.kind || r.attrs["job_id"] != event.JobID || r.attrs["attempt"] != event.Attempt {
				t.Fatalf("runtime transition or diagnostics changed: %v", r.attrs)
			}
			if !slices.Equal(r.labels, []string{"operation", "event", "code"}) || !slices.Equal(r.values, []string{"refresh", tt.kind, want.Code.String()}) {
				t.Fatalf("unexpected metric dimensions: labels=%v values=%v", r.labels, r.values)
			}
			if strings.Contains(fmt.Sprint(r.failure, r.fields, r.attrs), "private") {
				t.Fatal("observation leaked runtime or handler error payload")
			}
		})
	}
	jobmw.EventLogging(nil)(t.Context(), job.Event{Kind: "attempt_finished", JobType: "refresh"})
	jobmw.EventMetrics(nil)(t.Context(), job.Event{Kind: "attempt_finished", JobType: "refresh"})
	jobmw.EventTracing(nil)(t.Context(), job.Event{Kind: "attempt_finished", JobType: "refresh"})
}
