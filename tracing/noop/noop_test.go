package noop_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/tracing/noop"
	"github.com/nostosx/servex/observability/tracing"
)

// recordingSpan is a minimal Span used to observe context propagation.
type recordingSpan struct {
	mu    sync.Mutex
	ended bool
	attrs map[string]any
	err   error
}

func (s *recordingSpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
}

func (s *recordingSpan) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = make(map[string]any)
	}
	s.attrs[key] = value
}

func (s *recordingSpan) RecordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (*recordingSpan) TraceID() string { return "trace" }
func (*recordingSpan) SpanID() string  { return "span" }

// mapCarrier is a minimal Carrier.
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string { return c[key] }
func (c mapCarrier) Set(key, value string) { c[key] = value }
func (c mapCarrier) Keys() []string        { return slices.Sorted(maps.Keys(c)) }

func TestNoopTracer(t *testing.T) {
	tr := noop.New()
	if tr == nil {
		t.Fatal("Noop returned nil")
	}

	t.Run("without parent span", func(t *testing.T) {
		parent := t.Context()
		ctx, span := tr.Start(parent, "op", tracing.SpanKindServer)
		if ctx != parent {
			t.Fatal("Start must return ctx unchanged when it carries no span")
		}
		if span == nil || span.TraceID() != "" || span.SpanID() != "" {
			t.Fatalf("unexpected span %v", span)
		}
		span.SetAttribute("k", "v")
		span.RecordError(errors.New("x"))
		span.End()
	})

	t.Run("with recording parent span", func(t *testing.T) {
		parent := &recordingSpan{}
		ctx, span := tr.Start(tracing.ContextWithSpan(t.Context(), parent), "op", tracing.SpanKindClient)
		span.RecordError(errors.New("x"))
		tracing.SpanFromContext(ctx).SetAttribute("k", "v")
		if parent.err != nil || parent.attrs != nil {
			t.Fatal("calls on the started span leaked into the parent span")
		}
		if tracing.SpanFromContext(ctx) == tracing.Span(parent) {
			t.Fatal("context must carry the started span, not the parent")
		}
	})

	t.Run("with noop parent span", func(t *testing.T) {
		_, parentSpan := tr.Start(t.Context(), "parent", tracing.SpanKindInternal)
		parent := tracing.ContextWithSpan(t.Context(), parentSpan)
		ctx, _ := tr.Start(parent, "op", tracing.SpanKindInternal)
		if ctx != parent {
			t.Fatal("Start must not rewrap a context that already carries a noop span")
		}
	})

	t.Run("nil context", func(t *testing.T) {
		var nilCtx context.Context
		ctx, span := tr.Start(nilCtx, "op", tracing.SpanKindInternal)
		if ctx == nil || span == nil {
			t.Fatal("Start(nil) must return a usable context and span")
		}
	})
}

func TestNoopPropagator(t *testing.T) {
	p := noop.NewPropagator()
	if p == nil {
		t.Fatal("NoopPropagator returned nil")
	}
	c := mapCarrier{"existing": "v"}
	ctx := tracing.ContextWithSpan(t.Context(), &recordingSpan{})
	p.Inject(ctx, c)
	if len(c) != 1 || c.Get("existing") != "v" {
		t.Fatalf("Inject modified the carrier: %v", c)
	}
	if got := p.Extract(t.Context(), mapCarrier{"traceparent": "00-abc-def-01"}); got != t.Context() {
		t.Fatal("Extract must return ctx unchanged")
	}
}

func TestNoopDoesNotAllocate(t *testing.T) {
	tr := noop.New()
	p := noop.NewPropagator()
	ctx := t.Context()
	c := mapCarrier{}
	err := errors.New("x")
	allocs := testing.AllocsPerRun(100, func() {
		sctx, span := tr.Start(ctx, "op", tracing.SpanKindServer)
		span.SetAttribute("k", 1)
		span.RecordError(err)
		span.End()
		_ = tracing.SpanFromContext(sctx)
		p.Inject(sctx, c)
		_ = p.Extract(sctx, c)
		_ = noop.New()
		_ = noop.NewPropagator()
	})
	if allocs != 0 {
		t.Fatalf("noop tracing allocated %v times per run, want 0", allocs)
	}
}

func TestNoopConcurrentUse(t *testing.T) {
	tr := noop.New()
	p := noop.NewPropagator()
	parent := tracing.ContextWithSpan(t.Context(), &recordingSpan{})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			ctx, span := tr.Start(parent, "op", tracing.SpanKind(i%5))
			defer span.End()
			span.SetAttribute("i", i)
			p.Inject(ctx, mapCarrier{})
			_ = tracing.SpanFromContext(p.Extract(ctx, mapCarrier{}))
		})
	}
	wg.Wait()
}

func BenchmarkNoopStart(b *testing.B) {
	tr := noop.New()
	ctx := b.Context()
	for b.Loop() {
		_, span := tr.Start(ctx, "op", tracing.SpanKindServer)
		span.End()
	}
}
