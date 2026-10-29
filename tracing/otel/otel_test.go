package otel_test

import (
	"context"
	"errors"
	"math"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"

	"github.com/nostosx/servex-contrib/tracing/otel"
)

// harness bundles an SDK tracer provider with an in-memory exporter.
type harness struct {
	exporter *tracetest.InMemoryExporter
	otel     oteltrace.Tracer
	tracer   tracing.Tracer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ot := provider.Tracer("test")
	return &harness{exporter: exporter, otel: ot, tracer: otel.NewTracer(ot)}
}

// span returns the single ended span named name.
func (h *harness) span(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range h.exporter.GetSpans() {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d ended spans named %q, want 1", len(found), name)
	}
	return found[0]
}

var (
	traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	spanIDPattern  = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

func TestSpanKindMapping(t *testing.T) {
	tests := []struct {
		kind tracing.SpanKind
		want oteltrace.SpanKind
	}{
		{tracing.SpanKindInternal, oteltrace.SpanKindInternal},
		{tracing.SpanKindServer, oteltrace.SpanKindServer},
		{tracing.SpanKindClient, oteltrace.SpanKindClient},
		{tracing.SpanKindProducer, oteltrace.SpanKindProducer},
		{tracing.SpanKindConsumer, oteltrace.SpanKindConsumer},
		{tracing.SpanKind(99), oteltrace.SpanKindInternal},
	}
	for _, tt := range tests {
		h := newHarness(t)
		_, s := h.tracer.Start(t.Context(), "op", tt.kind)
		s.End()
		if got := h.span(t, "op").SpanKind; got != tt.want {
			t.Errorf("kind %d mapped to %v, want %v", tt.kind, got, tt.want)
		}
	}
}

func TestSpanIDsAndContext(t *testing.T) {
	h := newHarness(t)
	ctx, s := h.tracer.Start(t.Context(), "op", tracing.SpanKindServer)
	defer s.End()

	if !traceIDPattern.MatchString(s.TraceID()) || !spanIDPattern.MatchString(s.SpanID()) {
		t.Fatalf("IDs %q/%q are not lowercase hex", s.TraceID(), s.SpanID())
	}
	if got := tracing.SpanFromContext(ctx); got != s {
		t.Fatal("tracing.SpanFromContext does not return the started span")
	}
	sc := oteltrace.SpanFromContext(ctx).SpanContext()
	if sc.TraceID().String() != s.TraceID() || sc.SpanID().String() != s.SpanID() {
		t.Fatalf("oteltrace.SpanFromContext = %s/%s, want %s/%s", sc.TraceID(), sc.SpanID(), s.TraceID(), s.SpanID())
	}
}

func TestSetAttribute(t *testing.T) {
	h := newHarness(t)
	_, s := h.tracer.Start(t.Context(), "attrs", tracing.SpanKindInternal)
	s.SetAttribute("string", "v")
	s.SetAttribute("bool", true)
	s.SetAttribute("int", 1)
	s.SetAttribute("int8", int8(-8))
	s.SetAttribute("int16", int16(16))
	s.SetAttribute("int32", int32(32))
	s.SetAttribute("int64", int64(64))
	s.SetAttribute("uint8", uint8(8))
	s.SetAttribute("uint16", uint16(16))
	s.SetAttribute("uint32", uint32(32))
	s.SetAttribute("uint", uint(7))
	s.SetAttribute("uint64.small", uint64(9))
	s.SetAttribute("uint64.big", uint64(math.MaxUint64))
	s.SetAttribute("float32", float32(1.5))
	s.SetAttribute("float64", 2.5)
	s.SetAttribute("strings", []string{"a", "b"})
	s.SetAttribute("bools", []bool{true, false})
	s.SetAttribute("ints", []int{1, 2})
	s.SetAttribute("int64s", []int64{3, 4})
	s.SetAttribute("float64s", []float64{0.5})
	s.SetAttribute("value", attribute.IntValue(42))
	s.SetAttribute("duration", 1500*time.Millisecond)
	s.SetAttribute("error", errors.New("boom"))
	s.SetAttribute("stringer", stringer("str"))
	s.SetAttribute("other", struct{ A int }{A: 1})
	s.SetAttribute("nil", nil)
	s.End()

	want := map[attribute.Key]attribute.Value{
		"string":       attribute.StringValue("v"),
		"bool":         attribute.BoolValue(true),
		"int":          attribute.IntValue(1),
		"int8":         attribute.Int64Value(-8),
		"int16":        attribute.Int64Value(16),
		"int32":        attribute.Int64Value(32),
		"int64":        attribute.Int64Value(64),
		"uint8":        attribute.Int64Value(8),
		"uint16":       attribute.Int64Value(16),
		"uint32":       attribute.Int64Value(32),
		"uint":         attribute.Int64Value(7),
		"uint64.small": attribute.Int64Value(9),
		"uint64.big":   attribute.StringValue("18446744073709551615"),
		"float32":      attribute.Float64Value(1.5),
		"float64":      attribute.Float64Value(2.5),
		"strings":      attribute.StringSliceValue([]string{"a", "b"}),
		"bools":        attribute.BoolSliceValue([]bool{true, false}),
		"ints":         attribute.IntSliceValue([]int{1, 2}),
		"int64s":       attribute.Int64SliceValue([]int64{3, 4}),
		"float64s":     attribute.Float64SliceValue([]float64{0.5}),
		"value":        attribute.IntValue(42),
		"duration":     attribute.StringValue("1.5s"),
		"error":        attribute.StringValue("boom"),
		"stringer":     attribute.StringValue("str"),
		"other":        attribute.StringValue("{1}"),
	}
	got := map[attribute.Key]attribute.Value{}
	for _, kv := range h.span(t, "attrs").Attributes {
		got[kv.Key] = kv.Value
	}
	if len(got) != len(want) {
		t.Errorf("got %d attributes, want %d: %v", len(got), len(want), got)
	}
	for k, w := range want {
		if g, ok := got[k]; !ok || g.Type() != w.Type() || g.String() != w.String() {
			t.Errorf("attribute %q = %v (%v), want %v (%v)", k, g.String(), g.Type(), w.String(), w.Type())
		}
	}
}

type stringer string

func (s stringer) String() string { return string(s) }

func TestRecordError(t *testing.T) {
	h := newHarness(t)
	_, s := h.tracer.Start(t.Context(), "fail", tracing.SpanKindClient)
	s.RecordError(nil) // ignored
	s.RecordError(errors.New("boom"))
	s.End()

	got := h.span(t, "fail")
	if got.Status.Code != codes.Error || got.Status.Description != "boom" {
		t.Fatalf("status = %+v, want Error/boom", got.Status)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "exception" {
		t.Fatalf("events = %+v, want one exception event", got.Events)
	}
}

func TestParentFromOpenTelemetrySpan(t *testing.T) {
	h := newHarness(t)
	ctx, parent := h.otel.Start(t.Context(), "otel-parent")
	_, child := h.tracer.Start(ctx, "child", tracing.SpanKindInternal)
	child.End()
	parent.End()

	got := h.span(t, "child")
	if got.Parent.SpanID() != parent.SpanContext().SpanID() || got.SpanContext.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("child parent = %s, want %s", got.Parent.SpanID(), parent.SpanContext().SpanID())
	}
}

func TestOpenTelemetryChildOfServexSpan(t *testing.T) {
	h := newHarness(t)
	ctx, parent := h.tracer.Start(t.Context(), "servex-parent", tracing.SpanKindServer)
	_, child := h.otel.Start(ctx, "otel-child")
	child.End()
	parent.End()

	got := h.span(t, "otel-child")
	if got.Parent.SpanID().String() != parent.SpanID() || got.SpanContext.TraceID().String() != parent.TraceID() {
		t.Fatalf("otel child parent = %s, want %s", got.Parent.SpanID(), parent.SpanID())
	}
}

func TestParentFromServexKeyOnly(t *testing.T) {
	h := newHarness(t)
	_, parent := h.tracer.Start(t.Context(), "parent", tracing.SpanKindServer)
	// A context that carries the span only under the servex key.
	ctx := tracing.ContextWithSpan(context.Background(), parent)
	_, child := h.tracer.Start(ctx, "child", tracing.SpanKindInternal)
	child.End()
	parent.End()

	if got := h.span(t, "child"); got.Parent.SpanID().String() != parent.SpanID() {
		t.Fatalf("child parent = %s, want %s", got.Parent.SpanID(), parent.SpanID())
	}

	md := metadata.MD{}
	otel.NewPropagator(nil).Inject(ctx, md)
	if want := "00-" + parent.TraceID() + "-" + parent.SpanID() + "-01"; md.Get("traceparent") != want {
		t.Fatalf("traceparent = %q, want %q", md.Get("traceparent"), want)
	}
}

func TestPropagationThroughMetadata(t *testing.T) {
	h := newHarness(t)
	prop := otel.NewPropagator(propagation.TraceContext{})

	clientCtx, client := h.tracer.Start(t.Context(), "client", tracing.SpanKindClient)
	md := metadata.MD{}
	prop.Inject(clientCtx, md)
	client.End()

	want := "00-" + client.TraceID() + "-" + client.SpanID() + "-01"
	if got := md.Get("traceparent"); got != want {
		t.Fatalf("traceparent = %q, want %q", got, want)
	}

	// Server side: a fresh context, as a transport would create.
	serverCtx := prop.Extract(context.Background(), md.Clone())
	remote := tracing.SpanFromContext(serverCtx)
	if remote.TraceID() != client.TraceID() || remote.SpanID() != client.SpanID() {
		t.Fatalf("extracted span = %s/%s, want %s/%s", remote.TraceID(), remote.SpanID(), client.TraceID(), client.SpanID())
	}
	remote.SetAttribute("ignored", 1) // non-recording; must not panic
	remote.End()

	serverCtx, server := h.tracer.Start(serverCtx, "server", tracing.SpanKindServer)
	server.End()
	if tracing.SpanFromContext(serverCtx) != server {
		t.Fatal("tracing.SpanFromContext does not return the server span")
	}

	got := h.span(t, "server")
	if got.SpanContext.TraceID().String() != client.TraceID() {
		t.Fatalf("server trace = %s, want %s", got.SpanContext.TraceID(), client.TraceID())
	}
	if got.Parent.SpanID().String() != client.SpanID() || !got.Parent.IsRemote() {
		t.Fatalf("server parent = %s (remote %v), want remote %s", got.Parent.SpanID(), got.Parent.IsRemote(), client.SpanID())
	}
}

func TestCompositePropagator(t *testing.T) {
	h := newHarness(t)
	prop := otel.NewPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx, s := h.tracer.Start(baggage.ContextWithBaggage(t.Context(), bag), "op", tracing.SpanKindClient)
	defer s.End()

	md := metadata.MD{}
	prop.Inject(ctx, md)
	if md.Get("baggage") != "tenant=acme" || md.Get("traceparent") == "" {
		t.Fatalf("injected metadata = %v", md)
	}
	got := prop.Extract(context.Background(), md)
	if baggage.FromContext(got).Member("tenant").Value() != "acme" {
		t.Fatal("baggage not extracted")
	}
}

func TestExtractWithoutTraceContext(t *testing.T) {
	prop := otel.NewPropagator(nil)
	parent := t.Context()
	for _, c := range []tracing.Carrier{nil, metadata.MD{}, metadata.Pairs("traceparent", "garbage")} {
		ctx := prop.Extract(parent, c)
		if tracing.SpanFromContext(ctx) != nil {
			t.Fatalf("Extract(%v) produced a span", c)
		}
		if oteltrace.SpanContextFromContext(ctx).IsValid() {
			t.Fatalf("Extract(%v) produced a span context", c)
		}
	}
	if ctx := prop.Extract(parent, nil); ctx != parent {
		t.Fatal("Extract with nil carrier must return ctx unchanged")
	}
}

func TestNilArguments(t *testing.T) {
	var nilCtx context.Context

	tr := otel.NewTracer(nil)
	ctx, s := tr.Start(nilCtx, "op", tracing.SpanKindServer)
	if ctx == nil || s == nil {
		t.Fatal("Start(nil) must return a usable context and span")
	}
	if s.TraceID() != "" || s.SpanID() != "" {
		t.Fatalf("noop-backed span has IDs %q/%q", s.TraceID(), s.SpanID())
	}
	s.SetAttribute("k", "v")
	s.RecordError(errors.New("x"))
	s.End()

	prop := otel.NewPropagator(nil)
	prop.Inject(nilCtx, metadata.MD{})
	prop.Inject(t.Context(), nil)
	if prop.Extract(nilCtx, metadata.MD{}) == nil {
		t.Fatal("Extract(nil ctx) returned nil")
	}
}

func TestInjectWithoutSpan(t *testing.T) {
	md := metadata.MD{}
	otel.NewPropagator(nil).Inject(t.Context(), md)
	if len(md) != 0 {
		t.Fatalf("Inject without a span wrote %v", md)
	}
}

func TestConcurrentUse(t *testing.T) {
	h := newHarness(t)
	prop := otel.NewPropagator(nil)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			ctx, s := h.tracer.Start(t.Context(), "conc", tracing.SpanKind(i%5))
			s.SetAttribute("i", i)
			md := metadata.MD{}
			prop.Inject(ctx, md)
			_, child := h.tracer.Start(prop.Extract(context.Background(), md), "conc-child", tracing.SpanKindServer)
			child.End()
			s.End()
		})
	}
	wg.Wait()
	names := make([]string, 0, 64)
	for _, s := range h.exporter.GetSpans() {
		names = append(names, s.Name)
	}
	if len(names) != 64 || slices.Index(names, "conc-child") < 0 {
		t.Fatalf("exported %d spans, want 64", len(names))
	}
}
