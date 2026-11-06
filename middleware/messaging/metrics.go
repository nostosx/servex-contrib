package messaging

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/observability/metrics"
)

// MetricsOption configures duration histogram buckets.
type MetricsOption = observe.MetricsOption

// WithMetricBuckets copies explicit histogram bucket upper bounds in seconds.
func WithMetricBuckets(b []float64) MetricsOption { return observe.WithMetricBuckets(b) }

// PublishMetrics observes one backend-confirmed publish call.
func PublishMetrics(meter metrics.Meter, opts ...MetricsOption) messaging.PublishMiddleware {
	begin := metricBegin("message_publish", meter, nil, opts...)
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

// ConsumeMetrics observes one processing attempt; acknowledgement remains separate.
func ConsumeMetrics(meter metrics.Meter, opts ...MetricsOption) messaging.ConsumeMiddleware {
	begin := metricBegin("message_process", meter, nil, opts...)
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

// SettlementMetrics observes native acknowledgement and commit results independently of handler outcomes.
func SettlementMetrics(meter metrics.Meter, opts ...MetricsOption) messaging.SettlementObserver {
	begin := metricBegin("message_settlement", meter, []string{"action"}, opts...)
	return func(ctx context.Context, event messaging.Settlement) {
		handler := errorx.Classify(event.HandlerError)
		_, finish := begin(ctx, event.Topic, "action", event.Action, "group", event.Group,
			"message_id", event.MessageID, "handler_code", handler.Code.String(), "handler_reason", handler.Reason)
		finish(event.Error, true)
	}
}

func metricBegin(name string, meter metrics.Meter, labels []string, opts ...MetricsOption) observe.Begin {
	return observe.Metrics(name, meter, observe.MetricBuckets(metrics.DefaultBuckets, opts...), labels...)
}
