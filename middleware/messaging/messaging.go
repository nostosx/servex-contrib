// Package messaging provides native publish and delivery-attempt policies.
// Acknowledgement and redelivery remain owned by broker implementations.
package messaging

import (
	"context"
	"time"
	"uuid"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
)

// Identity assigns a producer identity when absent. It must wrap any publish
// retry policy so repeated attempts use one ID. No automatic retry is installed.
func Identity() messaging.PublishMiddleware {
	return func(next messaging.PublishFunc) messaging.PublishFunc {
		return func(ctx context.Context, message *messaging.Message) error {
			if message == nil {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_REQUIRED", "message required")
			}
			copy := *message
			if copy.ID == "" {
				copy.ID = uuid.NewV7().String()
			}
			return next(ctx, &copy)
		}
	}
}

// Timeout bounds processing cooperatively on the caller goroutine. It never
// retries or acknowledges work. The handler must observe context cancellation.
func Timeout(duration time.Duration) messaging.ConsumeMiddleware {
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, message *messaging.Message) error {
			if duration <= 0 {
				return next(ctx, message)
			}
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			return next(ctx, message)
		}
	}
}

// Recovery classifies a processing panic as a non-retryable Internal failure.
func Recovery() messaging.ConsumeMiddleware {
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, message *messaging.Message) (err error) {
			defer func() {
				if recover() != nil {
					err = errorx.New(errorx.Internal, "MESSAGE_PANIC", "message processing panicked")
				}
			}()
			return next(ctx, message)
		}
	}
}
