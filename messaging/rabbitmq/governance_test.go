package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/messaging"
)

type failingAck struct{ err error }

func (a failingAck) Ack(uint64, bool) error        { return a.err }
func (a failingAck) Nack(uint64, bool, bool) error { return a.err }
func (a failingAck) Reject(uint64, bool) error     { return a.err }

func TestIdentityAndSettlementFailureAreIndependentOfProcessing(t *testing.T) {
	t.Parallel()
	msg := &messaging.Message{ID: "stable-id", Topic: "orders", Body: []byte("payload")}
	published := toPublishing(msg)
	delivery := amqp.Delivery{MessageId: published.MessageId, RoutingKey: msg.Topic, Body: published.Body}
	if got := toMessage(delivery); got.ID != msg.ID {
		t.Fatal("message ID lost")
	}
	if got := redelivery(delivery, msg.Topic, 1); got.MessageId != msg.ID {
		t.Fatal("redelivery changed identity")
	}
	cause := errors.New("ack failed")
	delivery.Acknowledger = failingAck{cause}
	var observed messaging.Settlement
	s := &subscription{group: "billing", b: &Broker{opts: options{settlement: func(_ context.Context, event messaging.Settlement) { observed = event }}}}
	if err := s.handle(t.Context(), func(ctx context.Context, _ *messaging.Message) error {
		info, ok := messaging.DeliveryFromContext(ctx)
		if !ok || info.Group != "billing" {
			t.Fatal("delivery context lost")
		}
		return nil
	}, delivery); err != nil {
		t.Fatal(err)
	}
	if observed.Action != "ack" || !errors.Is(observed.Error, cause) || observed.HandlerError != nil || observed.MessageID != msg.ID {
		t.Fatalf("settlement=%+v", observed)
	}
}
