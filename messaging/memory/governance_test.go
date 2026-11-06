package memory

import (
	"context"
	"testing"

	"github.com/nostosx/servex/messaging"
)

func TestIdentityAndNativeSettlement(t *testing.T) {
	t.Parallel()
	var event messaging.Settlement
	b := New(WithSettlementObserver(func(_ context.Context, got messaging.Settlement) { event = got }))
	defer b.Close()
	msg := cloneMessage(&messaging.Message{ID: "request-id", Topic: "orders", Body: []byte("payload")})
	if !b.handle(t.Context(), func(ctx context.Context, _ *messaging.Message) error {
		info, ok := messaging.DeliveryFromContext(ctx)
		if !ok || info.LocalAttempt != 1 {
			t.Fatal("missing local attempt")
		}
		return nil
	}, msg) || event.Action != "ack" || event.MessageID != msg.ID {
		t.Fatalf("event=%+v", event)
	}
}
