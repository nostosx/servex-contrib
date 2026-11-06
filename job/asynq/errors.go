package asynq

import (
	"context"
	"errors"
	"fmt"

	"github.com/nostosx/servex/observability/logger"

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

// asynqLogger adapts the servex logger to asynq.Logger.
type asynqLogger struct{ l logger.Logger }

func (l asynqLogger) Debug(args ...any) {
	l.log(context.Background(), logger.LevelDebug, fmt.Sprint(args...), "component", "asynq")
}
func (l asynqLogger) Info(args ...any) {
	l.log(context.Background(), logger.LevelInfo, fmt.Sprint(args...), "component", "asynq")
}
func (l asynqLogger) Warn(args ...any) {
	l.log(context.Background(), logger.LevelWarn, fmt.Sprint(args...), "component", "asynq")
}
func (l asynqLogger) Error(args ...any) {
	l.log(context.Background(), logger.LevelError, fmt.Sprint(args...), "component", "asynq")
}

// Fatal logs at error level. It does not exit the process: failures are
// reported through the errors returned by Run.
func (l asynqLogger) Fatal(args ...any) {
	l.log(context.Background(), logger.LevelError, fmt.Sprint(args...), "component", "asynq")
}

func (l asynqLogger) log(ctx context.Context, level logger.Level, message string, fields ...any) {
	if l.l != nil {
		l.l.Log(ctx, level, message, fields...)
	}
}
