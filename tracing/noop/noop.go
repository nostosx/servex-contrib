package noop

import (
	"context"

	"github.com/nostosx/servex/observability/tracing"
)

// New returns a tracing.Tracer whose spans record nothing. It is safe for
// concurrent use and avoids allocation when ctx carries no span.
func New() tracing.Tracer { return noopTracer{} }

// NewPropagator returns a tracing.Propagator that neither injects nor extracts
// anything. It is safe for concurrent use.
func NewPropagator() tracing.Propagator { return noopPropagator{} }

// noopTracer is a tracing.Tracer that starts no-op spans.
type noopTracer struct{}

var (
	_ tracing.Tracer     = noopTracer{}
	_ tracing.Span       = noopSpan{}
	_ tracing.Propagator = noopPropagator{}
)

// Start returns a no-op span. If ctx carries a span other than a no-op span,
// the returned context carries the no-op span instead so that
// SpanFromContext reflects the span just started; otherwise ctx is returned
// unchanged. An absent span remains absent in that context.
func (noopTracer) Start(ctx context.Context, _ string, _ tracing.SpanKind) (context.Context, tracing.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s := tracing.SpanFromContext(ctx); s != nil {
		if _, isNoop := s.(noopSpan); !isNoop {
			ctx = tracing.ContextWithSpan(ctx, noopSpan{})
		}
	}
	return ctx, noopSpan{}
}

// noopSpan is a tracing.Span that records nothing and has an invalid span context.
type noopSpan struct{}

func (noopSpan) End()                     {}
func (noopSpan) SetAttribute(string, any) {}
func (noopSpan) RecordError(error)        {}
func (noopSpan) TraceID() string          { return "" }
func (noopSpan) SpanID() string           { return "" }

// noopPropagator is a tracing.Propagator that does nothing.
type noopPropagator struct{}

func (noopPropagator) Inject(context.Context, tracing.Carrier) {}

func (noopPropagator) Extract(ctx context.Context, _ tracing.Carrier) context.Context { return ctx }
