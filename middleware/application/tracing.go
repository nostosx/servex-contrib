package application

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	nooptracing "github.com/nostosx/servex-contrib/observability/tracing/noop"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/tracing"
)

// Span attribute names used by this execution scope.
const (
	AttrProtocol  = "servex.protocol"
	AttrOperation = "servex.operation"
	AttrCode      = "servex.code"
	AttrReason    = "servex.reason"
	AttrRetryable = "servex.retryable"
	AttrRemote    = "servex.remote"
	AttrKind      = "servex.kind"
)

// Tracing observes one typed business invocation without changing its result or error.
func Tracing[Request, Result any](tracer tracing.Tracer) application.Middleware[Request, Result] {
	if tracer == nil {
		tracer = nooptracing.New()
	}
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, req Request) (result Result, err error) {
			ctx, span := tracer.Start(ctx, meta.Operation, tracing.SpanKindInternal)
			ctx = tracing.ContextWithSpan(ctx, span)

			finished := false
			defer func() {
				defer span.End()
				class := observe.Outcome(err, finished)
				observe.Record(span, class, errorx.IsServerFault(class.Code))
			}()
			span.SetAttribute(AttrKind, string(meta.Kind))
			span.SetAttribute(AttrOperation, meta.Operation)
			result, err = next.Handle(ctx, req)
			finished = true
			return result, err
		})
	}
}
