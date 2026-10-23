package otel

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/nostosx/servex/observability/tracing"
)

// tracer is a tracing.Tracer backed by an OpenTelemetry tracer.
type tracer struct {
	t oteltrace.Tracer
}

var _ tracing.Tracer = tracer{}

// NewTracer returns a tracing.Tracer that starts spans with t. A nil t
// yields a tracer whose spans record nothing.
//
// Start uses the OpenTelemetry span in ctx as the parent. If ctx carries no
// valid OpenTelemetry span context but its servex span was started by a
// Tracer from this package, that span is the parent instead. The returned
// context carries the new span for both oteltrace.SpanFromContext and
// tracing.SpanFromContext.
//
// tracing.SpanFromContext reads a key that OpenTelemetry does not maintain:
// after code calls the OpenTelemetry API directly to start a span, it keeps
// returning the last span stored by this package (or by Extract) until the
// next Start.
func NewTracer(t oteltrace.Tracer) tracing.Tracer {
	if t == nil {
		t = noop.Tracer{}
	}
	return tracer{t: t}
}

// Start implements tracing.Tracer.
func (t tracer) Start(ctx context.Context, name string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	ctx = otelContext(ctx)
	ctx, s := t.t.Start(ctx, name, oteltrace.WithSpanKind(spanKind(kind)))
	sp := &span{s: s}
	return tracing.ContextWithSpan(ctx, sp), sp
}

// otelContext returns ctx, never nil, carrying the parent span for
// OpenTelemetry. When ctx has no valid OpenTelemetry span context but its
// servex span comes from this package, that span is installed for
// OpenTelemetry.
func otelContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if oteltrace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	if s, ok := tracing.SpanFromContext(ctx).(*span); ok {
		return oteltrace.ContextWithSpan(ctx, s.s)
	}
	return ctx
}

// spanKind maps a servex span kind to its OpenTelemetry equivalent. Unknown
// kinds map to SpanKindInternal.
func spanKind(k tracing.SpanKind) oteltrace.SpanKind {
	switch k {
	case tracing.SpanKindServer:
		return oteltrace.SpanKindServer
	case tracing.SpanKindClient:
		return oteltrace.SpanKindClient
	case tracing.SpanKindProducer:
		return oteltrace.SpanKindProducer
	case tracing.SpanKindConsumer:
		return oteltrace.SpanKindConsumer
	default:
		return oteltrace.SpanKindInternal
	}
}

// span is a tracing.Span backed by an OpenTelemetry span.
type span struct {
	s oteltrace.Span
}

var _ tracing.Span = (*span)(nil)

// End implements tracing.Span.
func (s *span) End() { s.s.End() }

// SetAttribute implements tracing.Span. Strings, booleans, integers, floats,
// their slices, attribute.Value, time.Duration and fmt.Stringer values are
// converted to the matching attribute type; an unsigned integer above
// math.MaxInt64 and any other type are recorded as strings. A nil value is
// ignored.
func (s *span) SetAttribute(key string, value any) {
	if value == nil {
		return
	}
	s.s.SetAttributes(toAttribute(key, value))
}

// RecordError implements tracing.Span. It records err as an exception event
// and sets the span status to Error with err's message. A nil err is ignored.
func (s *span) RecordError(err error) {
	if err == nil {
		return
	}
	s.s.RecordError(err)
	s.s.SetStatus(codes.Error, err.Error())
}

// TraceID implements tracing.Span. It returns the 32-character lowercase
// hex trace ID, or "" if the span context is invalid.
func (s *span) TraceID() string {
	sc := s.s.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanID implements tracing.Span. It returns the 16-character lowercase hex
// span ID, or "" if the span context is invalid.
func (s *span) SpanID() string {
	sc := s.s.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// toAttribute converts value to an attribute for key.
func toAttribute(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int(key, v)
	case int8:
		return attribute.Int64(key, int64(v))
	case int16:
		return attribute.Int64(key, int64(v))
	case int32:
		return attribute.Int64(key, int64(v))
	case int64:
		return attribute.Int64(key, v)
	case uint8:
		return attribute.Int64(key, int64(v))
	case uint16:
		return attribute.Int64(key, int64(v))
	case uint32:
		return attribute.Int64(key, int64(v))
	case uint:
		return uintAttribute(key, uint64(v))
	case uint64:
		return uintAttribute(key, v)
	case uintptr:
		return uintAttribute(key, uint64(v))
	case float32:
		return attribute.Float64(key, float64(v))
	case float64:
		return attribute.Float64(key, v)
	case []string:
		return attribute.StringSlice(key, v)
	case []bool:
		return attribute.BoolSlice(key, v)
	case []int:
		return attribute.IntSlice(key, v)
	case []int64:
		return attribute.Int64Slice(key, v)
	case []float64:
		return attribute.Float64Slice(key, v)
	case attribute.Value:
		return attribute.KeyValue{Key: attribute.Key(key), Value: v}
	case time.Duration:
		return attribute.String(key, v.String())
	case error:
		return attribute.String(key, v.Error())
	case fmt.Stringer:
		return attribute.String(key, v.String())
	default:
		return attribute.String(key, fmt.Sprint(v))
	}
}

// uintAttribute records v as an int64 when it fits, and as a decimal string
// otherwise.
func uintAttribute(key string, v uint64) attribute.KeyValue {
	if v <= math.MaxInt64 {
		return attribute.Int64(key, int64(v))
	}
	return attribute.String(key, fmt.Sprint(v))
}

// propagator is a tracing.Propagator backed by an OpenTelemetry
// TextMapPropagator.
type propagator struct {
	p propagation.TextMapPropagator
}

var _ tracing.Propagator = propagator{}

// NewPropagator returns a tracing.Propagator that injects and extracts trace
// context with p, such as propagation.TraceContext{} or a composite built
// with propagation.NewCompositeTextMapPropagator. A nil p uses
// propagation.TraceContext{}.
//
// Inject writes the OpenTelemetry span context of ctx; when ctx has none but
// carries a servex span from this package, that span is injected. Extract
// returns a context whose remote span context becomes the parent of the next
// span started by a Tracer from this package; the context also carries a
// non-recording servex span for it, so tracing.SpanFromContext reports the
// extracted trace and span IDs.
func NewPropagator(p propagation.TextMapPropagator) tracing.Propagator {
	if p == nil {
		p = propagation.TraceContext{}
	}
	return propagator{p: p}
}

// Inject implements tracing.Propagator. A nil carrier is ignored.
func (p propagator) Inject(ctx context.Context, c tracing.Carrier) {
	if c == nil {
		return
	}
	p.p.Inject(otelContext(ctx), carrier{c})
}

// Extract implements tracing.Propagator. A nil carrier leaves ctx unchanged,
// as does a carrier without a valid span context.
func (p propagator) Extract(ctx context.Context, c tracing.Carrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return ctx
	}
	before := oteltrace.SpanContextFromContext(ctx)
	ctx = p.p.Extract(ctx, carrier{c})
	if sc := oteltrace.SpanContextFromContext(ctx); sc.IsValid() && !sc.Equal(before) {
		ctx = tracing.ContextWithSpan(ctx, &span{s: oteltrace.SpanFromContext(ctx)})
	}
	return ctx
}

// carrier adapts a tracing.Carrier to propagation.TextMapCarrier.
type carrier struct {
	c tracing.Carrier
}

var _ propagation.TextMapCarrier = carrier{}

func (c carrier) Get(key string) string { return c.c.Get(key) }
func (c carrier) Set(key, value string) { c.c.Set(key, value) }
func (c carrier) Keys() []string        { return c.c.Keys() }
