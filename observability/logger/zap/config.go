package zap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	upstream "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ErrInvalidConfig identifies invalid file-decodable logger settings.
var ErrInvalidConfig = errors.New("zap: invalid logger configuration")

// Config contains serializable logger settings. Empty strings use the defaults;
// Output selects stdout, stderr, or an append-only file. Runtime writers and
// native Zap cores are supplied through WithWriter and Wrap respectively.
type Config struct {
	Level     string `json:"level"`
	Format    string `json:"format"`
	Output    string `json:"output"`
	AddSource bool   `json:"add_source"`
}

// DefaultConfig selects info-level JSON records on stderr without source locations.
func DefaultConfig() Config {
	return Config{Level: "info", Format: "json", Output: "stderr"}
}

func (c Config) normalized() Config {
	c.Level = strings.ToLower(strings.TrimSpace(c.Level))
	c.Format = strings.ToLower(strings.TrimSpace(c.Format))
	if c.Level == "" {
		c.Level = "info"
	}
	if c.Format == "" {
		c.Format = "json"
	}
	if c.Output == "" {
		c.Output = "stderr"
	}
	return c
}

// Validate checks settings without opening output files or changing global state.
func (c Config) Validate() error {
	c = c.normalized()
	if _, err := c.level(); err != nil {
		return err
	}
	if c.Format != "json" && c.Format != "text" {
		return fmt.Errorf("%w: format", ErrInvalidConfig)
	}
	if strings.ContainsRune(c.Output, 0) {
		return fmt.Errorf("%w: output", ErrInvalidConfig)
	}
	return nil
}

func (c Config) level() (zapcore.Level, error) {
	switch c.Level {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return 0, fmt.Errorf("%w: level", ErrInvalidConfig)
	}
}

// Option supplies runtime dependencies to New.
type Option func(*options)
type options struct {
	writer    io.Writer
	hasWriter bool
}

// WithWriter replaces the configured output with a borrowed writer. Close never
// closes it; a writer implementing Sync is flushed. Nil is rejected.
func WithWriter(writer io.Writer) Option {
	return func(o *options) { o.writer, o.hasWriter = writer, true }
}

type standardOutput struct{ io.Writer }

func (standardOutput) Sync() error { return nil }

// New constructs a logger from configuration. It validates before opening an
// output, creates files with mode 0600, and owns only files it opens itself.
// Call Close after all producers stop; construction never changes Zap globals.
func New(config Config, opts ...Option) (*Logger, error) {
	config = config.normalized()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.hasWriter && nilWriter(o.writer) {
		return nil, fmt.Errorf("%w: writer", ErrInvalidConfig)
	}
	var output *os.File
	var writer zapcore.WriteSyncer
	if o.hasWriter {
		writer = zapcore.AddSync(o.writer)
		if file, ok := o.writer.(*os.File); ok && (file == os.Stdout || file == os.Stderr) {
			writer = standardOutput{file}
		}
	} else {
		switch config.Output {
		case "stdout":
			writer = standardOutput{os.Stdout}
		case "stderr":
			writer = standardOutput{os.Stderr}
		default:
			var err error
			output, err = os.OpenFile(config.Output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return nil, fmt.Errorf("zap: open output: %w", err)
			}
			writer = output
		}
	}
	encoderConfig := upstream.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.CallerKey = "source"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	var encoder zapcore.Encoder
	if config.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}
	level, _ := config.level()
	nativeOptions := []upstream.Option{}
	if config.AddSource {
		nativeOptions = append(nativeOptions, upstream.AddCaller(), upstream.AddCallerSkip(1))
	}
	backend := upstream.New(zapcore.NewCore(encoder, zapcore.Lock(writer), level), nativeOptions...)
	return &Logger{log: backend, output: output, managed: true}, nil
}

// Close flushes a New-created logger and closes its owned file once. Wrapped
// backends remain caller-owned, including their Sync. Standard streams are never
// closed or fsynced; borrowed writer Sync errors are retained.
func (l *Logger) Close() error {
	l.once.Do(func() {
		if !l.managed {
			return
		}
		l.closeErr = l.log.Sync()
		if l.output != nil {
			l.closeErr = errors.Join(l.closeErr, l.output.Close())
		}
	})
	return l.closeErr
}

func nilWriter(writer io.Writer) bool {
	if writer == nil {
		return true
	}
	value := reflect.ValueOf(writer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
