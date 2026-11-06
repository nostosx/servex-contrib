package slog

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
)

// ErrInvalidConfig identifies invalid file-decodable logger settings.
var ErrInvalidConfig = errors.New("slog: invalid logger configuration")

// Config contains serializable logger settings. Empty strings use the defaults;
// Output selects stdout, stderr, or an append-only file. Runtime writers and
// custom handlers are supplied through WithWriter and Wrap respectively.
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

func (c Config) level() (slog.Level, error) {
	switch c.Level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
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

// WithWriter replaces the configured output with a borrowed writer. New and
// Close never close it. A nil writer is rejected during construction.
func WithWriter(writer io.Writer) Option {
	return func(o *options) { o.writer, o.hasWriter = writer, true }
}

// New constructs a logger from configuration. It validates before opening an
// output, creates files with mode 0600, and owns only files it opens itself.
// Call Close after all producers stop; construction never changes slog.Default.
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
	writer := o.writer
	if !o.hasWriter {
		switch config.Output {
		case "stdout":
			writer = os.Stdout
		case "stderr":
			writer = os.Stderr
		default:
			var err error
			output, err = os.OpenFile(config.Output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return nil, fmt.Errorf("slog: open output: %w", err)
			}
			writer = output
		}
	}
	level, _ := config.level()
	handlerOptions := &slog.HandlerOptions{Level: level, AddSource: config.AddSource}
	var handler slog.Handler
	if config.Format == "json" {
		handler = slog.NewJSONHandler(writer, handlerOptions)
	} else {
		handler = slog.NewTextHandler(writer, handlerOptions)
	}
	return &Logger{log: slog.New(handler), output: output}, nil
}

// Close releases an owned output once. Wrapped backends, stdout/stderr and
// WithWriter outputs remain caller-owned. Producers must stop before Close.
func (l *Logger) Close() error {
	l.once.Do(func() {
		if l.output != nil {
			l.closeErr = l.output.Close()
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
