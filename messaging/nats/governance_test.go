package nats

import (
	"testing"

	native "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/metadata"
)

type identityMessage struct {
	jetstream.Msg
	message *native.Msg
}

func (m identityMessage) Subject() string        { return m.message.Subject }
func (m identityMessage) Data() []byte           { return m.message.Data }
func (m identityMessage) Headers() native.Header { return m.message.Header }

func TestNativeMessageIdentityRoundTrip(t *testing.T) {
	t.Parallel()
	input := &messaging.Message{ID: "request-id", Topic: "orders", Key: "partition", Header: metadata.Pairs(messaging.HeaderMessageID, "conflicting-id")}
	got := toMessage(identityMessage{message: toNATS(input)})
	if got.ID != input.ID || got.Key != input.Key || got.Topic != input.Topic {
		t.Fatalf("roundtrip=%+v", got)
	}
}
