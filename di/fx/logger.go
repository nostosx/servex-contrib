package fx

import (
	"context"
	"time"

	"go.uber.org/fx/fxevent"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
)

// NewLogger supplies a native fxevent.Logger for fx.WithLogger. Nil disables
// event logging. Error messages, causes, stacks, module names and provided values
// are omitted; errors use payload-free errorx classification. Fx events carry no
// request context, so this adapter does not invent trace correlation.
func NewLogger(log logger.Logger) fxevent.Logger { return &eventLogger{log: log} }

type eventLogger struct{ log logger.Logger }

func (l *eventLogger) LogEvent(event fxevent.Event) {
	if l.log == nil || event == nil {
		return
	}
	name, function, duration, lifecycle, err := eventFields(event)
	level := logger.LevelDebug
	if lifecycle {
		level = logger.LevelInfo
	}
	if err != nil {
		level = logger.LevelError
	}
	ctx := context.Background()
	if !l.log.Enabled(ctx, level) {
		return
	}
	classification := errorx.Classify(err)
	fields := []any{"component", "fx", "event", name, "code", classification.Code.String()}
	if function != "" {
		fields = append(fields, "function", function)
	}
	if duration > 0 {
		fields = append(fields, "duration", duration)
	}
	if err != nil {
		if classification.Reason != "" {
			fields = append(fields, "reason", classification.Reason)
		}
		fields = append(fields, "retryable", classification.Retryable, "remote", classification.Remote)
	}
	l.log.Log(ctx, level, "fx event", fields...)
}

func eventFields(event fxevent.Event) (name, function string, duration time.Duration, lifecycle bool, err error) {
	switch e := event.(type) {
	case *fxevent.OnStartExecuting:
		return "start_hook", e.FunctionName, 0, false, nil
	case *fxevent.OnStartExecuted:
		return "start_hook_finished", e.FunctionName, e.Runtime, false, e.Err
	case *fxevent.OnStopExecuting:
		return "stop_hook", e.FunctionName, 0, false, nil
	case *fxevent.OnStopExecuted:
		return "stop_hook_finished", e.FunctionName, e.Runtime, false, e.Err
	case *fxevent.Supplied:
		return "supplied", "", 0, false, e.Err
	case *fxevent.Provided:
		return "provided", e.ConstructorName, 0, false, e.Err
	case *fxevent.Replaced:
		return "replaced", "", 0, false, e.Err
	case *fxevent.Decorated:
		return "decorated", e.DecoratorName, 0, false, e.Err
	case *fxevent.BeforeRun:
		return "constructor", e.Name, 0, false, nil
	case *fxevent.Run:
		return "constructor_finished", e.Name, e.Runtime, false, e.Err
	case *fxevent.Invoking:
		return "invoking", e.FunctionName, 0, false, nil
	case *fxevent.Invoked:
		return "invoked", e.FunctionName, 0, false, e.Err
	case *fxevent.Started:
		return "started", "", 0, true, e.Err
	case *fxevent.Stopping:
		return "stopping", "", 0, true, nil
	case *fxevent.Stopped:
		return "stopped", "", 0, true, e.Err
	case *fxevent.RollingBack:
		return "rolling_back", "", 0, true, e.StartErr
	case *fxevent.RolledBack:
		return "rolled_back", "", 0, true, e.Err
	case *fxevent.LoggerInitialized:
		return "logger_initialized", e.ConstructorName, 0, false, e.Err
	default:
		return "unknown", "", 0, false, nil
	}
}
