package endpoint

import (
	"context"
	"time"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
)

// Message is the message of every record written by the middleware.
const Message = "call"

// Attribute keys of the records written by the middleware.
const (
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

// Logging logs each call's metadata, handler duration and safe error classification.
// Failures include reason, retryable and remote; valid trace/span IDs correlate
// records with tracing. Request/result payloads and error text are never logged.
// Panic and Goexit are recorded as interrupted Internal outcomes and propagate.
// A nil log disables output but preserves WithLogLevel callback execution.
func Logging(log logger.Logger, opts ...LoggingOption) endpoint.Middleware {
	chooseLevel := observe.LogLevel(opts...)
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (resp any, err error) {
			start, finished := time.Now(), false
			defer func() {
				duration := time.Since(start)
				outcome := observe.Outcome(err, finished)
				level := chooseLevel(outcome.Code)
				if log == nil || !log.Enabled(ctx, level) {
					return
				}
				info, _ := metadata.FromContext(ctx)
				attrs := []any{
					KeyKind, string(info.Kind), KeyProtocol, info.Protocol,
					KeyOperation, info.Operation, KeyDuration, duration,
				}
				attrs = append(attrs, observe.Fields(ctx, outcome)...)
				log.Log(ctx, level, Message, attrs...)
			}()
			resp, err = next(ctx, req)
			finished = true
			return resp, err
		}
	}
}
