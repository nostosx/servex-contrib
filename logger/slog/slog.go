// Package slog implements the servex logger contract with log/slog.
package slog

import (
	"context"
	"log/slog"

	"github.com/nostosx/servex/logger"
)

// Logger delegates to a caller-owned slog.Logger. Configure handlers, level
// filtering, enrichment and output on that logger before passing it to New.
type Logger struct{ log *slog.Logger }

var _ logger.Logger = (*Logger)(nil)

// New wraps l. Nil selects slog.Default at construction time; it never changes
// the process default or takes ownership of the handler's output resources.
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{log: l}
}

func (l *Logger) Enabled(ctx context.Context, level logger.Level) bool {
	return l.log.Enabled(ctx, slog.Level(level))
}

func (l *Logger) Log(ctx context.Context, level logger.Level, message string, fields ...any) {
	l.log.Log(ctx, slog.Level(level), message, fields...)
}
