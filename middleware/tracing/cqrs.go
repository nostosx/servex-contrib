package tracing

import (
	"context"
	"errors"

	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/tracing"
)

// AttrKind is the command or query kind of an application operation.
const AttrKind = "servex.kind"

// Command starts an internal span named after the assembly operation. It uses
// the caller context as parent and passes the child context to the handler.
// Transport metadata and headers are untouched. A nil tracer uses tracing.Noop.
// Only kind, operation and code are recorded; error text, payloads and panic
// values are excluded. Failed calls record a code-only error. Panics mark the
// span Internal, end it exactly once and propagate unchanged.
func Command[Request, Result any](tracer tracing.Tracer) cqrs.CommandMiddleware[Request, Result] {
	if tracer == nil {
		tracer = tracing.Noop()
	}
	return func(meta cqrs.Metadata, next cqrs.Handler[Request, Result]) cqrs.Handler[Request, Result] {
		return cqrs.CommandFunc[Request, Result](func(ctx context.Context, req Request) (Result, error) {
			ctx, span := tracer.Start(ctx, meta.Operation, tracing.SpanKindInternal)
			defer span.End()
			span.SetAttribute(AttrKind, string(meta.Kind))
			span.SetAttribute(AttrOperation, meta.Operation)
			code := errorx.Internal
			defer func() {
				span.SetAttribute(AttrCode, code.String())
				if code != errorx.OK {
					span.RecordError(errors.New(code.String()))
				}
			}()
			result, err := next.Handle(ctx, req)
			code = errorx.CodeOf(err)
			return result, err
		})
	}
}

// Query traces a read-only application call with the same rules as Command.
func Query[Request, Result any](tracer tracing.Tracer) cqrs.QueryMiddleware[Request, Result] {
	return Command[Request, Result](tracer)
}
