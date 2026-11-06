package messaging

import (
	"context"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/storage/idempotency"
)

// Deduplicate atomically records successful processing with participating SQL
// writes. The key callback must derive trusted scope and a canonical digest;
// Message.ID is required and used as the token. ACK remains outside this scope.
func Deduplicate(store idempotency.Store, key func(context.Context, *messaging.Message) (idempotency.Key, error)) messaging.ConsumeMiddleware {
	if store == nil || key == nil {
		panic("messaging: nil idempotency dependency")
	}
	return func(next messaging.Handler) messaging.Handler {
		return func(ctx context.Context, msg *messaging.Message) error {
			if msg == nil || msg.ID == "" {
				return errorx.New(errorx.InvalidArgument, "MESSAGE_ID_REQUIRED", "stable message id required")
			}
			identity, err := key(ctx, msg)
			if err != nil {
				return err
			}
			identity.Token = msg.ID
			identity.Operation = "consume:" + msg.Topic
			_, err = store.Do(ctx, identity, func(txctx context.Context) ([]byte, error) { return nil, next(txctx, msg) })
			return err
		}
	}
}
