// Package zap implements the servex logger contract with go.uber.org/zap.
package zap

import (
	"context"

	upstream "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/nostosx/servex/logger"
)

// Logger delegates to a caller-owned Zap logger. The caller configures its
// core, output, sampling and fields and remains responsible for Sync.
type Logger struct{ log *upstream.Logger }

var _ logger.Logger = (*Logger)(nil)

// New wraps l; nil selects a no-op backend. It never changes Zap globals.
func New(l *upstream.Logger) *Logger {
	if l == nil {
		l = upstream.NewNop()
	}
	return &Logger{log: l}
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
