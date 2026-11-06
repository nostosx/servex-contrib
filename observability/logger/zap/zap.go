// Package zap implements the servex logger contract with go.uber.org/zap.
package zap

import (
	"context"
	"os"
	"sync"

	upstream "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/nostosx/servex/observability/logger"
)

// Logger records through a configured Zap logger. Construct it with New or
// Wrap and do not copy it; output ownership is determined at construction.
type Logger struct {
	log      *upstream.Logger
	output   *os.File
	managed  bool
	once     sync.Once
	closeErr error
}

var _ logger.Logger = (*Logger)(nil)

// Wrap wraps l; nil selects a no-op backend. It never changes Zap globals.
func Wrap(l *upstream.Logger) *Logger {
	if l == nil {
		l = upstream.NewNop()
	}
	return &Logger{log: l.WithOptions(upstream.AddCallerSkip(1))}
}

func levelOf(level logger.Level) zapcore.Level {
	switch {
	case level < logger.LevelInfo:
		return zapcore.DebugLevel
	case level < logger.LevelWarn:
		return zapcore.InfoLevel
	case level < logger.LevelError:
		return zapcore.WarnLevel
	default:
		return zapcore.ErrorLevel
	}
}

func (l *Logger) Enabled(_ context.Context, level logger.Level) bool {
	return l.log.Core().Enabled(levelOf(level))
}

func (l *Logger) Log(_ context.Context, level logger.Level, message string, fields ...any) {
	l.log.Sugar().Logw(levelOf(level), message, fields...)
}
