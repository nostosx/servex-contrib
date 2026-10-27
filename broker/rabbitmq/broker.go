package rabbitmq

import (
	"context"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
)

// Compile-time interface checks.
var (
	_ broker.Publisher       = (*Broker)(nil)
	_ broker.ReadySubscriber = (*Broker)(nil)
)

// errClosed is returned by Publish once the Broker is closed.
var errClosed = errorx.New(errorx.FailedPrecondition, "RABBITMQ_CLOSED", "rabbitmq: broker is closed")

// Broker implements broker.Publisher and broker.ReadySubscriber with RabbitMQ. Its
// methods are safe for concurrent use. Create one with New.
//
// Publish uses one channel in confirm mode, reopened after a failure. Every
// Subscribe call opens its own channel on the same connection. The Broker
// does not own the connection; closing it ends active subscriptions with an
// error.
type Broker struct {
	open   func() (amqpChannel, error) // opens a channel on the connection
	opts   options
	ctx    context.Context // canceled by Close
	cancel context.CancelFunc

	mu   sync.Mutex
	pub  amqpChannel                            // publishing channel in confirm mode, nil until needed
	subs map[context.Context]context.CancelFunc // contexts created by active Subscribe calls
}

// New creates a Broker that publishes and consumes through conn. It does not
// perform any request; exchanges and queues are declared when they are first
// needed.
func New(conn *amqp.Connection, opts ...Option) *Broker {
	return newBroker(dialer(conn), opts...)
}

// newBroker creates a Broker that opens its channels with open.
func newBroker(open func() (amqpChannel, error), opts ...Option) *Broker {
	o := options{exchange: defaultExchange, maxRedeliveries: defaultMaxRedeliveries, prefetch: defaultPrefetch}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.deadLetterExchange == "" {
		o.deadLetterExchange = o.exchange + ".dlx"
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{open: open, opts: o, ctx: ctx, cancel: cancel}
}

// Publish publishes msg to the exchange with msg.Topic as routing key and
// waits for the publisher confirmation. Messages are persistent. A message
// whose routing key matches no queue binding is dropped by the server.
func (b *Broker) Publish(ctx context.Context, msg *broker.Message) error {
	switch {
	case msg == nil:
		return invalid("publish: nil message")
	case msg.Topic == "":
		return invalid("publish: empty topic")
	}
	return b.publish(ctx, "publish", b.opts.exchange, msg.Topic, toPublishing(msg))
}

// Subscribe consumes topic as a member of group until ctx ends or the Broker
// is closed, returning nil in both cases, or until the channel or the
// connection is closed unexpectedly, in which case it returns a retryable
// errorx.Unavailable error. Unacknowledged messages of an ending subscription
// are requeued by the server. The context passed to h is canceled when the
// subscription ends.
//
// topic may be a topic exchange binding pattern, such as "orders.*". The
// durable queue of a group and its dead letter queue are declared if they do
// not exist yet; an existing queue must have been declared with the same
// arguments.
func (b *Broker) Subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	return b.subscribe(ctx, topic, group, h, nil)
}

// SubscribeReady implements broker.ReadySubscriber. It behaves like
// Subscribe and calls the non-nil ready once the queue is declared and bound
// and the server accepted the consumer, so messages published afterwards are
// delivered. It returns without calling ready when setup fails, for example
// because a queue exists with different arguments, or when ctx ends or the
// Broker is closed first.
func (b *Broker) SubscribeReady(ctx context.Context, topic, group string, h endpoint.Endpoint, ready func()) error {
	return b.subscribe(ctx, topic, group, h, ready)
}

// subscribe implements Subscribe and SubscribeReady; ready may be nil.
func (b *Broker) subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint, ready func()) error {
	switch {
	case topic == "":
		return invalid("subscribe: empty topic")
	case h == nil:
		return invalid("subscribe: nil handler")
	}
	ctx, cancel := context.WithCancel(ctx)
	b.mu.Lock()
	if b.ctx.Err() != nil || ctx.Err() != nil {
		b.mu.Unlock()
		cancel()
		return nil
	}
	if b.subs == nil {
		b.subs = make(map[context.Context]context.CancelFunc)
	}
	b.subs[ctx] = cancel
	b.mu.Unlock()
	defer func() {
		cancel()
		b.mu.Lock()
		delete(b.subs, ctx)
		b.mu.Unlock()
	}()

	s, err := b.newSubscription(topic, group)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer s.ch.Close()
	return s.run(ctx, topic, group, h, ready)
}

// Close closes the publishing channel. Afterwards Publish returns an error and
// active Subscribe calls return nil. It does not close the connection. Close
// is idempotent and always returns nil.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancel()
	// Cancel subscriptions before returning from Close. An AfterFunc on b.ctx
	// would run asynchronously, leaving handlers and setup alive after Close.
	for _, cancel := range b.subs {
		cancel()
	}
	if b.pub != nil {
		_ = b.pub.Close()
		b.pub = nil
	}
	return nil
}

// publish publishes p to exchange with routing key key and waits for the
// confirmation.
func (b *Broker) publish(ctx context.Context, op, exchange, key string, p amqp.Publishing) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	acked, err := ch.publishConfirmed(ctx, exchange, key, p)
	switch {
	case err != nil:
		return classify(ctx, op, err)
	case !acked && ch.IsClosed():
		// A channel that closes while confirmations are pending resolves
		// them as nacks.
		if b.ctx.Err() != nil {
			return errClosed
		}
		return unavailable(op+": channel closed before the confirmation", amqp.ErrClosed)
	case !acked:
		return errorx.Newf(errorx.Unavailable, "RABBITMQ_NACKED", "rabbitmq: %s: message not confirmed by the broker", op)
	}
	return nil
}

// channel returns the publishing channel, opening it and declaring the
// exchange if needed.
func (b *Broker) channel() (amqpChannel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil {
		return nil, errClosed
	}
	if b.pub != nil && !b.pub.IsClosed() {
		return b.pub, nil
	}
	ch, err := b.open()
	if err != nil {
		return nil, classify(context.Background(), "open channel", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, classify(context.Background(), "enable confirms", err)
	}
	if err := declareExchange(ch, b.opts.exchange, amqp.ExchangeTopic); err != nil {
		_ = ch.Close()
		return nil, err
	}
	b.pub = ch
	return ch, nil
}

// declareExchange declares the durable exchange name of the given kind.
func declareExchange(ch amqpChannel, name, kind string) error {
	if err := ch.ExchangeDeclare(name, kind, true, false, false, false, nil); err != nil {
		return classify(context.Background(), "declare exchange "+name, err)
	}
	return nil
}

var (
	// groupEscaper escapes the group part of a queue name so that it contains
	// no dot.
	groupEscaper = strings.NewReplacer("%", "%25", ".", "%2E")
	// topicEscaper escapes the topic part of a queue name so that every
	// percent sign starts the escape "%25".
	topicEscaper = strings.NewReplacer("%", "%25")
)

// QueueName returns the name of the durable queue used by group for topic:
// the group and the topic joined by a dot, where "%" is escaped as "%25" in
// both and "." as "%2E" in the group, so that the first dot separates them
// and distinct pairs get distinct names. For example, QueueName("orders.*",
// "billing") is "billing.orders.*" and QueueName("b.c", "a") differs from
// QueueName("c", "a.b"), which is "a%2Eb.c".
//
// The queue name is also the routing key of the dead letters of the queue;
// QueueName(topic, "") is the one of a broadcast subscription. It starts with
// a dot and therefore never equals the name of a group queue.
func QueueName(topic, group string) string {
	return groupEscaper.Replace(group) + "." + topicEscaper.Replace(topic)
}

// DeadLetterQueueName returns the name of the durable queue that collects the
// dead letters of QueueName(topic, group): that name followed by "%dlq". The
// suffix is not a valid escape, so it never equals the name of a group queue.
func DeadLetterQueueName(topic, group string) string {
	return QueueName(topic, group) + "%dlq"
}
