package metrics

import (
	"context"
	"slices"
	"time"

	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/metrics"
)

// Application metric names, separate from transport request metrics.
const (
	UsecaseCallsTotal = "servex_usecase_calls_total"
	UsecaseDuration   = "servex_usecase_duration_seconds"
)

// Command records typed application calls. Calls are labelled kind, operation,
// code; durations in seconds are labelled kind, operation. Operation comes from
// ChainCommand or ChainQuery and must be a stable name, never user input.
// Instruments are declared once per factory. A nil meter uses metrics.Noop.
// Panics count as Internal failures and propagate unchanged. WithBuckets applies.
func Command[Request, Result any](meter metrics.Meter, opts ...Option) cqrs.CommandMiddleware[Request, Result] {
	if meter == nil {
		meter = metrics.Noop()
	}
	o := options{buckets: slices.Clone(metrics.DefaultBuckets)}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	calls := meter.Counter(UsecaseCallsTotal, "Total application calls by kind, operation and result code.", "kind", "operation", "code")
	durations := meter.Histogram(UsecaseDuration, "Application call durations in seconds by kind and operation.", o.buckets, "kind", "operation")
	return func(meta cqrs.Metadata, next cqrs.Handler[Request, Result]) cqrs.Handler[Request, Result] {
		return cqrs.CommandFunc[Request, Result](func(ctx context.Context, req Request) (Result, error) {
			start, code := time.Now(), errorx.Internal
			defer func() {
				calls.With(string(meta.Kind), meta.Operation, code.String()).Add(ctx, 1)
				durations.With(string(meta.Kind), meta.Operation).Observe(ctx, time.Since(start).Seconds())
			}()
			result, err := next.Handle(ctx, req)
			code = errorx.CodeOf(err)
			return result, err
		})
	}
}

// Query records a read-only application call with the same rules as Command.
func Query[Request, Result any](meter metrics.Meter, opts ...Option) cqrs.QueryMiddleware[Request, Result] {
	return Command[Request, Result](meter, opts...)
}
