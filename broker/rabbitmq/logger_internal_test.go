//go:build integration

package rabbitmq

import (
	"context"
	"log/slog"

	sxlog "github.com/nostosx/servex/logger"
)

// loggerFixture is a test double for the backend-independent contract.
type loggerFixture struct{ *slog.Logger }

func testLogger(l *slog.Logger) *loggerFixture { return &loggerFixture{l} }
func (l *loggerFixture) Enabled(ctx context.Context, level sxlog.Level) bool {
	return l.Logger.Enabled(ctx, slog.Level(level))
}
func (l *loggerFixture) Log(ctx context.Context, level sxlog.Level, msg string, fields ...any) {
	l.Logger.Log(ctx, slog.Level(level), msg, fields...)
}
