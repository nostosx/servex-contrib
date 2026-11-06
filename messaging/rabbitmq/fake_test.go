package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeServer stands in for a RabbitMQ connection in unit tests. It records
// the topology the Broker declares, the messages it publishes and how it
// settles deliveries, and fails operations on demand. One mutex guards the
// server and all of its channels.
type fakeServer struct {
	mu    sync.Mutex
	fails map[string]error // by operation, see op
	nack  bool             // publications are not confirmed
	// dropConfirms closes the channel after a publication, before its
	// confirmation arrives.
	dropConfirms bool
	onPublish    func() // called before a publication is checked
	channels     []*fakeChannel
	exchanges    map[string]string // kind by name
	queues       map[string]fakeQueue
	bindings     []string // "exchange key -> queue"
	prefetch     int
	published    []publication
	settled      []string // "ack N", "requeue N" or "dead N" by delivery tag
	consumers    []*fakeChannel
	named        int // server-named queues so far
}

// fakeQueue is a declared queue.
type fakeQueue struct {
	durable, autoDelete, exclusive bool
	args                           amqp.Table
}

// publication is a message published through the fake server.
type publication struct {
	exchange, key string
	msg           amqp.Publishing
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		fails:     make(map[string]error),
		exchanges: make(map[string]string),
		queues:    make(map[string]fakeQueue),
	}
}

// fail makes operation op fail with err; a nil err lets it succeed again.
// Operations are "open", "confirm", "qos", "consume", "publish",
// "exchange <name>", "queue <name>" and "bind <queue>".
func (s *fakeServer) fail(op string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[op] = err
}

// op reports the injected failure of op. s.mu must be held.
func (s *fakeServer) op(op string) error {
	return s.fails[op]
}

func (s *fakeServer) open() (amqpChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.op("open"); err != nil {
		return nil, err
	}
	ch := &fakeChannel{srv: s}
	s.channels = append(s.channels, ch)
	return ch, nil
}

// consumer returns the channel that started consuming last.
func (s *fakeServer) consumer() *fakeChannel {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.consumers) == 0 {
		panic("no consumer")
	}
	return s.consumers[len(s.consumers)-1]
}

// openChannels returns how many channels are open.
func (s *fakeServer) openChannels() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ch := range s.channels {
		if !ch.closed {
			n++
		}
	}
	return n
}

// publications returns the messages published so far.
func (s *fakeServer) publications() []publication {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.published)
}

// settlements returns how the deliveries were settled so far.
func (s *fakeServer) settlements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.settled)
}

// fakeChannel is a channel of a fakeServer. It is also the Acknowledger of
// the deliveries it hands out.
type fakeChannel struct {
	srv *fakeServer

	// Guarded by srv.mu.
	closed     bool
	confirm    bool
	closes     []chan *amqp.Error
	cancels    []chan string
	deliveries chan amqp.Delivery // open while consuming
	tag        uint64
}

var _ amqpChannel = (*fakeChannel)(nil)

// call checks that the channel is open and that op does not fail.
// ch.srv.mu must be held.
func (ch *fakeChannel) call(op string) error {
	if ch.closed {
		return amqp.ErrClosed
	}
	return ch.srv.op(op)
}

func (ch *fakeChannel) IsClosed() bool {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	return ch.closed
}

func (ch *fakeChannel) Close() error {
	ch.shutdown(nil)
	return nil
}

func (ch *fakeChannel) Confirm(bool) error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("confirm"); err != nil {
		return err
	}
	ch.confirm = true
	return nil
}

func (ch *fakeChannel) ExchangeDeclare(name, kind string, _, _, _, _ bool, _ amqp.Table) error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("exchange " + name); err != nil {
		return err
	}
	ch.srv.exchanges[name] = kind
	return nil
}

func (ch *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, _ bool, args amqp.Table) (amqp.Queue, error) {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("queue " + name); err != nil {
		return amqp.Queue{}, err
	}
	if name == "" {
		ch.srv.named++
		name = fmt.Sprintf("amq.gen-%d", ch.srv.named)
	}
	ch.srv.queues[name] = fakeQueue{durable: durable, autoDelete: autoDelete, exclusive: exclusive, args: args}
	return amqp.Queue{Name: name}, nil
}

func (ch *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("bind " + name); err != nil {
		return err
	}
	ch.srv.bindings = append(ch.srv.bindings, exchange+" "+key+" -> "+name)
	return nil
}

func (ch *fakeChannel) Qos(prefetchCount, _ int, _ bool) error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("qos"); err != nil {
		return err
	}
	ch.srv.prefetch = prefetchCount
	return nil
}

func (ch *fakeChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if err := ch.call("consume"); err != nil {
		return nil, err
	}
	// Buffered so that deliver never blocks while holding the lock.
	ch.deliveries = make(chan amqp.Delivery, 16)
	ch.srv.consumers = append(ch.srv.consumers, ch)
	return ch.deliveries, nil
}

func (ch *fakeChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if ch.closed {
		close(c)
	} else {
		ch.closes = append(ch.closes, c)
	}
	return c
}

func (ch *fakeChannel) NotifyCancel(c chan string) chan string {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if ch.closed {
		close(c)
	} else {
		ch.cancels = append(ch.cancels, c)
	}
	return c
}

func (ch *fakeChannel) publishConfirmed(ctx context.Context, exchange, key string, p amqp.Publishing) (bool, error) {
	ch.srv.mu.Lock()
	hook := ch.srv.onPublish
	ch.srv.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ch.srv.mu.Lock()
	if err := ch.call("publish"); err != nil {
		ch.srv.mu.Unlock()
		return false, err
	}
	if !ch.confirm {
		ch.srv.mu.Unlock()
		return false, errors.New("fake: publish on a channel without confirms")
	}
	ch.srv.published = append(ch.srv.published, publication{exchange: exchange, key: key, msg: p})
	nack, drop := ch.srv.nack, ch.srv.dropConfirms
	ch.srv.mu.Unlock()
	if drop {
		// amqp091-go resolves the confirmations pending on a closing
		// channel as nacks.
		ch.shutdown(&amqp.Error{Code: amqp.ChannelError, Reason: "closed before the confirmation"})
		return false, nil
	}
	return !nack, nil
}

// deliver hands d to the consumer of the channel and returns its delivery
// tag.
func (ch *fakeChannel) deliver(d amqp.Delivery) uint64 {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	return ch.deliverLocked(d)
}

// deliverLocked enqueues a delivery while the caller owns the server lock.
func (ch *fakeChannel) deliverLocked(d amqp.Delivery) uint64 {
	ch.tag++
	d.DeliveryTag, d.Acknowledger = ch.tag, ch
	ch.deliveries <- d
	return ch.tag
}

// shutdown closes the channel as amqp091-go does (Channel.shutdown and
// closeResources): listeners registered with NotifyClose receive err, if
// any, then the deliveries and all notification channels, including those of
// NotifyCancel, are closed.
func (ch *fakeChannel) shutdown(err *amqp.Error) {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	if ch.closed {
		return
	}
	ch.closed = true
	for _, c := range ch.closes {
		if err != nil {
			c <- err
		}
	}
	ch.stopConsuming()
	for _, c := range ch.closes {
		close(c)
	}
	for _, c := range ch.cancels {
		close(c)
	}
}

// cancelConsumer cancels the consumer from the server side: listeners
// registered with NotifyCancel receive tag and the deliveries end, while the
// channel stays open.
func (ch *fakeChannel) cancelConsumer(tag string) {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	for _, c := range ch.cancels {
		c <- tag
	}
	ch.stopConsuming()
}

// endDeliveries closes the deliveries without any notification.
func (ch *fakeChannel) endDeliveries() {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	ch.stopConsuming()
}

// stopConsuming closes the deliveries. ch.srv.mu must be held.
func (ch *fakeChannel) stopConsuming() {
	if ch.deliveries != nil {
		close(ch.deliveries)
		ch.deliveries = nil
	}
}

func (ch *fakeChannel) settle(what string, tag uint64) error {
	ch.srv.mu.Lock()
	defer ch.srv.mu.Unlock()
	ch.srv.settled = append(ch.srv.settled, fmt.Sprintf("%s %d", what, tag))
	return nil
}

func (ch *fakeChannel) Ack(tag uint64, _ bool) error { return ch.settle("ack", tag) }

func (ch *fakeChannel) Nack(tag uint64, _ bool, requeue bool) error {
	if requeue {
		return ch.settle("requeue", tag)
	}
	return ch.settle("dead", tag)
}

func (ch *fakeChannel) Reject(tag uint64, requeue bool) error { return ch.Nack(tag, false, requeue) }
