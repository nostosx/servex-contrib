package asynq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nostosx/servex/errorx"
)

// invalid returns a non-retryable InvalidArgument error.
func invalid(format string, args ...any) error {
	return errorx.Newf(errorx.InvalidArgument, "ASYNQ_INVALID_ARGUMENT", "asynq: "+format, args...)
}

// classify converts an error returned by Redis or asynq during op into an
// errorx error, keeping err as the cause. Errors caused by the end of ctx keep
// the context error so that errorx reports Canceled or DeadlineExceeded;
// everything else is a retryable Unavailable error, as it means that Redis
// could not be reached or did not answer.
func classify(ctx context.Context, op string, err error) error {
	if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return fmt.Errorf("asynq: %s: %w", op, ctx.Err())
	}
	return errorx.Newf(errorx.Unavailable, "ASYNQ_UNAVAILABLE", "asynq: %s: redis unavailable", op).WithCause(err)
}

// logger adapts a slog.Logger to asynq.Logger.
type logger struct{ l *slog.Logger }

func (l logger) Debug(args ...any) { l.l.Debug(fmt.Sprint(args...), "component", "asynq") }
func (l logger) Info(args ...any)  { l.l.Info(fmt.Sprint(args...), "component", "asynq") }
func (l logger) Warn(args ...any)  { l.l.Warn(fmt.Sprint(args...), "component", "asynq") }
func (l logger) Error(args ...any) { l.l.Error(fmt.Sprint(args...), "component", "asynq") }

// Fatal logs at error level. It does not exit the process: failures are
// reported through the errors returned by Run.
func (l logger) Fatal(args ...any) { l.l.Error(fmt.Sprint(args...), "component", "asynq") }
