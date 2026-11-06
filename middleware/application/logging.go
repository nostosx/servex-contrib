package application

import (
	"context"
	"time"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/observability/logger"
)

// Shared log field names.
const (
	Message      = "call"
	KeyKind      = "kind"
	KeyProtocol  = "protocol"
	KeyOperation = "operation"
	KeyDuration  = "duration"
	KeyCode      = "code"
	KeyReason    = "reason"
	KeyRetryable = "retryable"
	KeyRemote    = "remote"
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
)

// LoggingOption configures log levels for this execution scope.
type LoggingOption = observe.LoggingOption

// WithLogLevel chooses a log level from the classified outcome.
func WithLogLevel(fn func(errorx.Code) logger.Level) LoggingOption { return observe.WithLogLevel(fn) }

// Logging observes one typed business invocation without changing its result or error.
func Logging[Request, Result any](log logger.Logger, opts ...LoggingOption) application.Middleware[Request, Result] {
	chooseLevel := observe.LogLevel(opts...)
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, req Request) (result Result, err error) {
			start, finished := time.Now(), false
			defer func() {
				duration := time.Since(start)
				outcome := observe.Outcome(err, finished)
				level := chooseLevel(outcome.Code)
				if log == nil || !log.Enabled(ctx, level) {
					return
				}
				attrs := []any{
					KeyKind, string(meta.Kind), KeyOperation, meta.Operation,
					KeyDuration, duration,
				}
				attrs = append(attrs, observe.Fields(ctx, outcome)...)
				log.Log(ctx, level, Message, attrs...)
			}()
			result, err = next.Handle(ctx, req)
			finished = true
			return result, err
		})
	}
}
