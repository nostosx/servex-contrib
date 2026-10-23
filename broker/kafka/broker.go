package kafka

import (
	"context"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// Headers added to dead letter records.
const (
	// HeaderOriginalTopic holds the topic the dead lettered record was
	// consumed from.
	HeaderOriginalTopic = "x-servex-original-topic"
	// HeaderError holds the text of the last handler error.
	HeaderError = "x-servex-error"
)

// Compile-time interface checks.
var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
)

// Broker implements broker.Publisher and broker.Subscriber with Kafka. Its
// methods are safe for concurrent use. Create one with New or NewFromClient.
//
// Publish produces through a single client. Every Subscribe call creates its
// own consumer client from the same client options, because a franz-go
// client belongs to at most one consumer group.
type Broker struct {
	producer *kgo.Client
	owned    bool      // whether Close closes producer
	base     []kgo.Opt // options of consumer clients, before overrides
	opts     options
	ctx      context.Context // canceled by Close
	cancel   context.CancelFunc
	once     sync.Once
}

// New creates a Broker whose clients are configured with kopts, which must at
// least name the seed brokers (kgo.SeedBrokers) and must not configure
// consumption. Creating the Broker does not connect to the cluster. Close
// closes the producing client.
func New(kopts []kgo.Opt, opts ...Option) (*Broker, error) {
	kopts = slices.Clone(kopts)
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, invalid("new client: %v", err)
	}
	b := newBroker(cl, kopts, opts)
	b.owned = true
	return b, nil
}

// NewFromClient creates a Broker that produces through client. Consumer
// clients are created from client.Opts(), so client must not be configured to
// consume. Close does not close client.
func NewFromClient(client *kgo.Client, opts ...Option) *Broker {
	return newBroker(client, slices.Clone(client.Opts()), opts)
}

func newBroker(cl *kgo.Client, base []kgo.Opt, opts []Option) *Broker {
	o := options{maxRedeliveries: defaultMaxRedeliveries, pingInterval: defaultPingInterval}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{producer: cl, base: base, opts: o, ctx: ctx, cancel: cancel}
}

// Publish produces msg to msg.Topic and waits for the acknowledgement
// configured on the client. An empty msg.Key produces a record without a key.
func (b *Broker) Publish(ctx context.Context, msg *broker.Message) error {
	switch {
	case msg == nil:
		return invalid("publish: nil message")
	case msg.Topic == "":
		return invalid("publish: empty topic")
	}
	if b.ctx.Err() != nil {
		return errClosed
	}
	if err := b.producer.ProduceSync(ctx, toRecord(msg)).FirstErr(); err != nil {
		return classify(ctx, "publish", err)
	}
	return nil
}

// Subscribe consumes topic as a member of group until ctx ends or the Broker
// is closed, returning nil in both cases. It returns an errorx.Unavailable
// error when the cluster stays unreachable (see WithPingInterval) and an
// error for fatal fetch or commit failures. The context passed to h is
// canceled when the subscription ends.
//
// With a non-empty group, offsets are committed once the records before them
// are settled; records that were not settled when the subscription ended are
// redelivered to the remaining members of the group. With an empty group the
// subscriber receives the records produced after Subscribe was called, and
// nothing is committed.
func (b *Broker) Subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	switch {
	case topic == "":
		return invalid("subscribe: empty topic")
	case h == nil:
		return invalid("subscribe: nil handler")
	}
	if b.ctx.Err() != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()

	s, err := b.newSubscription(topic, group)
	if err != nil {
		return err
	}
	return s.run(ctx, cancel, h)
}

// Close closes the Broker. Afterwards Publish returns an error and active
// Subscribe calls return nil. A client passed to NewFromClient is not
// closed. Close is idempotent and always returns nil.
func (b *Broker) Close() error {
	b.once.Do(func() {
		b.cancel()
		if b.owned {
			b.producer.Close()
		}
	})
	return nil
}

// toRecord converts msg to a Kafka record.
func toRecord(msg *broker.Message) *kgo.Record {
	rec := &kgo.Record{Topic: msg.Topic, Value: slices.Clone(msg.Body)}
	if msg.Key != "" {
		rec.Key = []byte(msg.Key)
	}
	for k, vs := range msg.Header.All() {
		for _, v := range vs {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}
	return rec
}

// toMessage converts a consumed record to a broker message.
func toMessage(rec *kgo.Record) *broker.Message {
	msg := &broker.Message{Topic: rec.Topic, Key: string(rec.Key), Body: slices.Clone(rec.Value)}
	if len(rec.Headers) > 0 {
		msg.Header = make(metadata.MD, len(rec.Headers))
		for _, h := range rec.Headers {
			msg.Header.Add(h.Key, string(h.Value))
		}
	}
	return msg
}
