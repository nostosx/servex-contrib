package kafka

import (
	"testing"

	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/metadata"
)

func TestMessageIdentityIsSeparateFromPartitionKey(t *testing.T) {
	t.Parallel()
	input := &messaging.Message{ID: "request-id", Key: "partition", Topic: "orders", Header: metadata.Pairs("tenant", "a", messaging.HeaderMessageID, "conflicting-id")}
	got := toMessage(toRecord(input))
	if got.ID != input.ID || got.Key != input.Key || got.Header.Get("tenant") != "a" || got.Header.Get(messaging.HeaderMessageID) != "" {
		t.Fatalf("roundtrip=%+v", got)
	}
}
