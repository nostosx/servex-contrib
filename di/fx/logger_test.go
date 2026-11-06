package fx_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	uberfx "go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	fxbridge "github.com/nostosx/servex-contrib/di/fx"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
)

type logRecording struct {
	enabled bool
	level   logger.Level
	fields  []any
	calls   int
}

func (r *logRecording) Enabled(context.Context, logger.Level) bool { return r.enabled }
func (r *logRecording) Log(_ context.Context, level logger.Level, message string, fields ...any) {
	r.calls++
	r.level = level
	r.fields = slices.Clone(fields)
	if message != "fx event" {
		panic("unexpected event message")
	}
}

type unsafeError struct{}

func (unsafeError) Error() string { panic("error text must not be formatted") }

func TestEventLoggerSafeClassificationAndLevels(t *testing.T) {
	failure := errorx.New(errorx.InvalidArgument, "CONFIG_INVALID", "private message").
		WithMetadata(map[string]string{"credential": "private metadata"}).WithCause(unsafeError{}).WithRemote()
	for _, tt := range []struct {
		name     string
		event    fxevent.Event
		wantName string
		level    logger.Level
	}{
		{"hook start", &fxevent.OnStartExecuting{FunctionName: "app.Open"}, "start_hook", logger.LevelDebug},
		{"hook started", &fxevent.OnStartExecuted{FunctionName: "app.Open", Runtime: time.Second}, "start_hook_finished", logger.LevelDebug},
		{"hook stop", &fxevent.OnStopExecuting{FunctionName: "app.Close"}, "stop_hook", logger.LevelDebug},
		{"hook stopped", &fxevent.OnStopExecuted{FunctionName: "app.Close", Err: failure}, "stop_hook_finished", logger.LevelError},
		{"supply", &fxevent.Supplied{TypeName: "private value", Err: failure}, "supplied", logger.LevelError},
		{"provide", &fxevent.Provided{ConstructorName: "app.Open", StackTrace: []string{"private stack"}, Err: failure}, "provided", logger.LevelError},
		{"replace", &fxevent.Replaced{Err: failure}, "replaced", logger.LevelError},
		{"decorate", &fxevent.Decorated{Err: failure}, "decorated", logger.LevelError},
		{"constructor", &fxevent.BeforeRun{Name: "app.Open", ModuleName: "private module"}, "constructor", logger.LevelDebug},
		{"constructor result", &fxevent.Run{Name: "app.Open", Err: failure, Runtime: time.Second}, "constructor_finished", logger.LevelError},
		{"invoke", &fxevent.Invoking{FunctionName: "app.Wire"}, "invoking", logger.LevelDebug},
		{"invoke result", &fxevent.Invoked{FunctionName: "app.Wire", Trace: "private stack", Err: failure}, "invoked", logger.LevelError},
		{"started", &fxevent.Started{}, "started", logger.LevelInfo},
		{"stopping", &fxevent.Stopping{}, "stopping", logger.LevelInfo},
		{"stopped", &fxevent.Stopped{Err: failure}, "stopped", logger.LevelError},
		{"rolling back", &fxevent.RollingBack{StartErr: failure}, "rolling_back", logger.LevelError},
		{"rolled back", &fxevent.RolledBack{}, "rolled_back", logger.LevelInfo},
		{"logger initialized", &fxevent.LoggerInitialized{ConstructorName: "app.Logger", Err: failure}, "logger_initialized", logger.LevelError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &logRecording{enabled: true}
			fxbridge.NewLogger(r).LogEvent(tt.event)
			fields := map[string]any{}
			for i := 0; i+1 < len(r.fields); i += 2 {
				fields[r.fields[i].(string)] = r.fields[i+1]
			}
			if r.calls != 1 || r.level != tt.level || fields["event"] != tt.wantName || fields["component"] != "fx" {
				t.Fatalf("unexpected event: level=%v fields=%v", r.level, r.fields)
			}
			if strings.Contains(fmt.Sprint(r.fields), "private") {
				t.Fatalf("payload escaped into log: %v", r.fields)
			}
			if tt.level == logger.LevelError && (fields["code"] != "InvalidArgument" || fields["reason"] != "CONFIG_INVALID" || fields["remote"] != true) {
				t.Fatalf("safe classification lost: %v", fields)
			}
		})
	}
}

func TestEventLoggerDisabledAndNativeFx(t *testing.T) {
	fxbridge.NewLogger(nil).LogEvent(&fxevent.Started{})
	r := &logRecording{}
	fxbridge.NewLogger(r).LogEvent(&fxevent.Started{Err: unsafeError{}})
	fxbridge.NewLogger(r).LogEvent(nil)
	if r.calls != 0 {
		t.Fatal("disabled logger was called")
	}
	r.enabled = true
	native := uberfx.New(uberfx.WithLogger(func() fxevent.Logger { return fxbridge.NewLogger(r) }))
	bridge := fxbridge.Hook("dependencies", native)
	if err := bridge.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := bridge.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if r.calls == 0 {
		t.Fatal("native Fx did not use injected logger")
	}
}
