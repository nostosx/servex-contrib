package endpoint_test

import (
	"context"
	"log/slog"

	"github.com/nostosx/servex/observability/logger"
)

// recoveryLoggerFixture is a test double for the backend-independent contract.
type recoveryLoggerFixture struct{ *slog.Logger }

func recoveryTestLogger(l *slog.Logger) *recoveryLoggerFixture { return &recoveryLoggerFixture{l} }
func (l *recoveryLoggerFixture) Enabled(ctx context.Context, level logger.Level) bool {
	return l.Logger.Enabled(ctx, slog.Level(level))
}
func (l *recoveryLoggerFixture) Log(ctx context.Context, level logger.Level, msg string, fields ...any) {
	l.Logger.Log(ctx, slog.Level(level), msg, fields...)
}
