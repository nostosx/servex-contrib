package logging

import (
	"context"
	"time"

	sxlog "github.com/nostosx/servex/logger"

	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/tracing"
)

// Command logs typed application calls using assembly metadata. It records kind,
// operation, duration, code and valid trace IDs, never requests, results, error
// text or panic values. Panics are recorded as Internal and propagate unchanged.
// A nil logger disables logging. WithLevel also applies to these records.
func Command[Request, Result any](logger sxlog.Logger, opts ...Option) cqrs.CommandMiddleware[Request, Result] {
	o := options{level: DefaultLevel}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(meta cqrs.Metadata, next cqrs.Handler[Request, Result]) cqrs.Handler[Request, Result] {
		return cqrs.CommandFunc[Request, Result](func(ctx context.Context, req Request) (Result, error) {
			start, code := time.Now(), errorx.Internal
			defer func() {
				level := o.level(code)
				if logger == nil || !logger.Enabled(ctx, level) {
					return
				}
				attrs := []any{
					KeyKind, string(meta.Kind), KeyOperation, meta.Operation,
					KeyDuration, time.Since(start), KeyCode, code.String(),
				}
				span := tracing.SpanFromContext(ctx)
				if span != nil && span.TraceID() != "" {
					attrs = append(attrs, "trace_id", span.TraceID(), "span_id", span.SpanID())
				}
				logger.Log(ctx, level, Message, attrs...)
			}()
			result, err := next.Handle(ctx, req)
			code = errorx.CodeOf(err)
			return result, err
		})
	}
}

// Query logs a read-only application call with the same rules as Command.
func Query[Request, Result any](logger sxlog.Logger, opts ...Option) cqrs.QueryMiddleware[Request, Result] {
	return Command[Request, Result](logger, opts...)
}
