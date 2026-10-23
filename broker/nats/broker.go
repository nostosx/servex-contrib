package nats

import (
	"context"
	"encoding/hex"
	"errors"
	"slices"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// HeaderKey is the NATS header that carries Message.Key. It is not exposed in
// Message.Header of consumed messages. Its capital letters keep it apart from
// metadata keys, which are always lower case.
const HeaderKey = "Servex-Key"

// Compile-time interface checks.
var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
)

// Broker implements broker.Publisher and broker.Subscriber with JetStream. Its
// methods are safe for concurrent use. Create one with New. The Broker does
// not own the connection of js; closing that connection ends active
// subscriptions with an error.
type Broker struct {
	js     jetstream.JetStream
	opts   options
	bucket *redeliveryBucket
}

// New creates a Broker that publishes and consumes through js.
func New(js jetstream.JetStream, opts ...Option) *Broker {
	o := options{maxRedeliveries: defaultMaxRedeliveries}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.backoff == nil {
		o.backoff = defaultBackoff
	}
	if o.ackWait <= 0 {
		o.ackWait = defaultAckWait
	}
	if o.bucket == "" {
		o.bucket = DefaultRedeliveryBucket
	}
	return &Broker{js: js, opts: o, bucket: &redeliveryBucket{js: js, name: o.bucket}}
}

// Publish publishes msg to the subject msg.Topic and waits for the stream to
// acknowledge it. It returns a non-retryable errorx.FailedPrecondition error
// if no stream is bound to the subject, and a retryable errorx.Unavailable
// error if the bound stream did not answer, for example during a leader
// election.
func (b *Broker) Publish(ctx context.Context, msg *broker.Message) error {
	switch {
	case msg == nil:
		return invalid("publish: nil message")
	case msg.Topic == "":
		return invalid("publish: empty topic")
	}
	if _, err := b.js.PublishMsg(ctx, toNATS(msg)); err != nil {
		if errors.Is(err, jetstream.ErrNoStreamResponse) && ctx.Err() == nil {
			// Nobody answered: either no stream is bound to the subject or
			// the stream is temporarily unavailable.
			if _, lookupErr := b.js.StreamNameBySubject(ctx, msg.Topic); errors.Is(lookupErr, jetstream.ErrStreamNotFound) {
				return noStream("publish", err)
			}
		}
		return classify(ctx, "publish", err)
	}
	return nil
}

// Subscribe consumes the subject topic as a member of group until ctx ends,
// returning nil, or until the subscription fails, for example because the
// connection was closed or the consumer was deleted.
//
// A non-empty group uses the durable consumer DurableName(topic, group),
// created or updated as needed, which starts with the oldest message of the
// stream when it is first created. An empty group uses an ephemeral consumer
// that receives the messages published after Subscribe created it and is
// deleted when Subscribe returns. The context passed to h is canceled when
// ctx ends.
func (b *Broker) Subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	switch {
	case topic == "":
		return invalid("subscribe: empty topic")
	case h == nil:
		return invalid("subscribe: nil handler")
	}
	s, err := b.newSubscription(ctx, topic, group)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer s.close(ctx)
	if fn := b.opts.subscribed; fn != nil {
		fn(topic, group)
	}
	return s.run(ctx, h)
}

// DurableName returns the name of the durable consumer used by group for
// topic: the hexadecimal UTF-8 bytes of group and topic joined by an
// underscore. The encoding preserves distinct pairs and uses only characters
// valid in consumer names.
func DurableName(topic, group string) string {
	return hex.EncodeToString([]byte(group)) + "_" + hex.EncodeToString([]byte(topic))
}

// toNATS converts msg to a NATS message.
func toNATS(msg *broker.Message) *natsgo.Msg {
	m := &natsgo.Msg{Subject: msg.Topic, Data: slices.Clone(msg.Body)}
	if len(msg.Header) > 0 || msg.Key != "" {
		m.Header = make(natsgo.Header, len(msg.Header)+1)
		for k, vs := range msg.Header.All() {
			m.Header[k] = slices.Clone(vs)
		}
		if msg.Key != "" {
			m.Header[HeaderKey] = []string{msg.Key}
		}
	}
	return m
}

// toMessage converts a consumed JetStream message to a broker message.
func toMessage(m jetstream.Msg) *broker.Message {
	msg := &broker.Message{Topic: m.Subject(), Body: slices.Clone(m.Data())}
	for k, vs := range m.Headers() {
		if k == HeaderKey {
			if len(vs) > 0 {
				msg.Key = vs[0]
			}
			continue
		}
		if msg.Header == nil {
			msg.Header = make(metadata.MD)
		}
		for _, v := range vs {
			msg.Header.Add(k, v)
		}
	}
	return msg
}
