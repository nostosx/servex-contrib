// Package slog implements the servex logger contract with log/slog.
package slog

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/nostosx/servex/observability/logger"
)

// Logger records through a configured slog.Logger. Construct it with New or
// Wrap and do not copy it; output ownership is determined at construction.
type Logger struct {
	log      *slog.Logger
	output   *os.File
	once     sync.Once
	closeErr error
}

var _ logger.Logger = (*Logger)(nil)

// Wrap wraps l. Nil selects slog.Default at construction time; it never changes
// the process default or takes ownership of the handler's output resources.
func Wrap(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{log: l}
}

func (l *Logger) Enabled(ctx context.Context, level logger.Level) bool {
	return l.log.Enabled(ctx, slog.Level(level))
}

func (l *Logger) Log(ctx context.Context, level logger.Level, message string, fields ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !l.log.Enabled(ctx, slog.Level(level)) {
		return
	}
	var callers [1]uintptr
	runtime.Callers(2, callers[:])
	record := slog.NewRecord(time.Now(), slog.Level(level), message, callers[0])
	record.Add(fields...)
	_ = l.log.Handler().Handle(ctx, record)
}
