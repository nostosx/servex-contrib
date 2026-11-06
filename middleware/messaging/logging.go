package messaging

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
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

// PublishLogging observes one backend-confirmed publish call.
func PublishLogging(log logger.Logger, opts ...LoggingOption) messaging.PublishMiddleware {
	begin := logBegin("message_publish", log, opts...)
	return func(next messaging.PublishFunc) messaging.PublishFunc {
		return func(ctx context.Context, message *messaging.Message) (err error) {
			if message == nil {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_REQUIRED", "message required")
			}

			ctx, finish := begin(ctx, message.Topic)
			finished := false
			defer func() { finish(err, finished) }()

			err = next(ctx, message)
			finished = true
			return err
		}
	}
}

// ConsumeLogging observes one processing attempt; acknowledgement remains separate.
func ConsumeLogging(log logger.Logger, opts ...LoggingOption) messaging.ConsumeMiddleware {
	begin := logBegin("message_process", log, opts...)
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, message *messaging.Message) (err error) {
			if message == nil {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_REQUIRED", "message required")
			}

			delivery, _ := messaging.DeliveryFromContext(ctx)
			ctx, finish := begin(ctx, message.Topic, "message_id", message.ID, "group", delivery.Group, "attempt", delivery.Attempt)
			finished := false
			defer func() { finish(err, finished) }()
			err = next(ctx, message)
			finished = true
			return err
		}
	}
}

// SettlementLogging observes native acknowledgement and commit results independently of handler outcomes.
func SettlementLogging(log logger.Logger, opts ...LoggingOption) messaging.SettlementObserver {
	begin := logBegin("message_settlement", log, opts...)
	return func(ctx context.Context, event messaging.Settlement) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.Topic, "action", event.Action, "group", event.Group,
			"message_id", event.MessageID, "handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}

func logBegin(name string, log logger.Logger, opts ...LoggingOption) observe.Begin {
	return observe.Logging(name, log, observe.LogLevel(opts...))
}
