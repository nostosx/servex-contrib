package messaging

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

// Span attribute names used by this execution scope.
const (
	AttrProtocol  = "servex.protocol"
	AttrOperation = "servex.operation"
	AttrCode      = "servex.code"
	AttrReason    = "servex.reason"
	AttrRetryable = "servex.retryable"
	AttrRemote    = "servex.remote"
)

// PublishTracing observes one backend-confirmed publish call.
func PublishTracing(tracer tracing.Tracer, propagation tracing.Propagator) messaging.PublishMiddleware {
	begin := observe.Tracing("message_publish", tracer, tracing.SpanKindProducer)
	return func(next messaging.PublishFunc) messaging.PublishFunc {
		return func(ctx context.Context, message *messaging.Message) (err error) {
			if message == nil {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_REQUIRED", "message required")
			}
			if tracer != nil {
				copy := *message
				copy.Header = message.Header.Clone()
				copy.Body = append([]byte(nil), message.Body...)
				if copy.Header == nil {
					copy.Header = make(metadata.MD)
				}
				message = &copy
			}
			ctx, finish := begin(ctx, message.Topic)
			finished := false
			defer func() { finish(err, finished) }()
			if tracer != nil && propagation != nil {
				propagation.Inject(ctx, message.Header)
			}
			err = next(ctx, message)
			finished = true
			return err
		}
	}
}

// ConsumeTracing observes one processing attempt; acknowledgement remains separate.
func ConsumeTracing(tracer tracing.Tracer, propagation tracing.Propagator) messaging.ConsumeMiddleware {
	begin := observe.Tracing("message_process", tracer, tracing.SpanKindConsumer)
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, message *messaging.Message) (err error) {
			if message == nil {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_REQUIRED", "message required")
			}
			if tracer != nil && propagation != nil {
				ctx = propagation.Extract(ctx, message.Header)
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

// SettlementTracing observes native acknowledgement and commit results independently of handler outcomes.
func SettlementTracing(tracer tracing.Tracer) messaging.SettlementObserver {
	begin := observe.Tracing("message_settlement", tracer, tracing.SpanKindInternal)
	return func(ctx context.Context, event messaging.Settlement) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.Topic, "action", event.Action, "group", event.Group,
			"message_id", event.MessageID, "handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}
