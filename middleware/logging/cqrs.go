package logging

import (
	"context"
	"log/slog"
	"time"

	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/tracing"
)

// Command logs typed application calls using assembly metadata. It records kind,
// operation, duration, code and valid trace IDs, never requests, results, error
// text or panic values. Panics are recorded as Internal and propagate unchanged.
// A nil logger uses slog.Default. WithLevel also applies to these records.
func Command[Request, Result any](logger *slog.Logger, opts ...Option) cqrs.CommandMiddleware[Request, Result] {
	if logger == nil {
		logger = slog.Default()
	}
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
				if !logger.Enabled(ctx, level) {
					return
				}
				attrs := []slog.Attr{
					slog.String(KeyKind, string(meta.Kind)), slog.String(KeyOperation, meta.Operation),
					slog.Duration(KeyDuration, time.Since(start)), slog.String(KeyCode, code.String()),
				}
				span := tracing.SpanFromContext(ctx)
				if id := span.TraceID(); id != "" {
					attrs = append(attrs, slog.String("trace_id", id), slog.String("span_id", span.SpanID()))
				}
				logger.LogAttrs(ctx, level, Message, attrs...)
			}()
			result, err := next.Handle(ctx, req)
			code = errorx.CodeOf(err)
			return result, err
		})
	}
}

// Query logs a read-only application call with the same rules as Command.
func Query[Request, Result any](logger *slog.Logger, opts ...Option) cqrs.QueryMiddleware[Request, Result] {
	return Command[Request, Result](logger, opts...)
}
