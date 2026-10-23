package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// subscription is one active Subscribe call with its own channel.
type subscription struct {
	b      *Broker
	ch     amqpChannel
	queue  string
	closed chan *amqp.Error
	cancel chan string
}

// newSubscription opens a channel and declares the exchanges, the queue and
// its bindings.
func (b *Broker) newSubscription(topic, group string) (*subscription, error) {
	ch, err := b.open()
	if err != nil {
		return nil, classify(context.Background(), "subscribe: open channel", err)
	}
	s := &subscription{
		b:      b,
		ch:     ch,
		closed: ch.NotifyClose(make(chan *amqp.Error, 1)),
		cancel: ch.NotifyCancel(make(chan string, 1)),
	}
	if err := s.declare(topic, group); err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err := ch.Qos(b.opts.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, classify(context.Background(), "subscribe: set prefetch", err)
	}
	return s, nil
}

// declare declares the exchanges and the queue of the subscription.
//
// Dead letters are published to the direct dead letter exchange with the
// routing key QueueName(topic, group), which is unique to the pair and free of
// binding pattern semantics, so that the dead letter queue of a group only
// receives the dead letters of its own queue.
func (s *subscription) declare(topic, group string) error {
	ch, o := s.ch, s.b.opts
	if err := declareExchange(ch, o.exchange, amqp.ExchangeTopic); err != nil {
		return err
	}
	if err := declareExchange(ch, o.deadLetterExchange, amqp.ExchangeDirect); err != nil {
		return err
	}
	key := QueueName(topic, group)
	args := amqp.Table{"x-dead-letter-exchange": o.deadLetterExchange, "x-dead-letter-routing-key": key}
	if group != "" {
		name := key
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			return classify(context.Background(), "subscribe: declare queue "+name, err)
		}
		dlq := DeadLetterQueueName(topic, group)
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return classify(context.Background(), "subscribe: declare queue "+dlq, err)
		}
		if err := ch.QueueBind(dlq, key, o.deadLetterExchange, false, nil); err != nil {
			return classify(context.Background(), "subscribe: bind queue "+dlq, err)
		}
		s.queue = name
	} else {
		// Broadcast: an exclusive queue of this subscription. No queue is
		// bound to its dead letter routing key by default.
		q, err := ch.QueueDeclare("", false, true, true, false, args)
		if err != nil {
			return classify(context.Background(), "subscribe: declare broadcast queue", err)
		}
		s.queue = q.Name
	}
	if err := ch.QueueBind(s.queue, topic, o.exchange, false, nil); err != nil {
		return classify(context.Background(), "subscribe: bind queue "+s.queue, err)
	}
	return nil
}

// run consumes until ctx ends or the channel fails.
func (s *subscription) run(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	if ctx.Err() != nil {
		return nil
	}
	deliveries, err := s.ch.Consume(s.queue, "", false, false, false, false, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return classify(context.Background(), "subscribe: consume", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	if fn := s.b.opts.subscribed; fn != nil {
		fn(topic, group)
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case d, ok := <-deliveries:
			// A delivery and cancellation may both be ready. Leave buffered
			// messages unacknowledged for the server to requeue when we close.
			if ctx.Err() != nil {
				return nil
			}
			if !ok {
				return s.lost()
			}
			if err := s.handle(ctx, h, d); err != nil {
				return err
			}
		}
	}
	return nil
}

// lost returns the error reported when deliveries stop unexpectedly. A
// closing channel sends its error, if any, before it closes the deliveries,
// and then closes its cancel notifications as well; the close error is
// therefore checked first, and a closed cancel notification is no
// cancellation.
func (s *subscription) lost() error {
	select {
	case aerr, ok := <-s.closed:
		if ok && aerr != nil {
			return classify(context.Background(), "subscribe: channel closed", aerr)
		}
	default:
	}
	select {
	case tag, ok := <-s.cancel:
		if ok {
			return errorx.Newf(errorx.Unavailable, "RABBITMQ_CONSUMER_CANCELED",
				"rabbitmq: subscribe: consumer %s canceled by the server", tag)
		}
	default:
	}
	return unavailable("subscribe: connection closed", amqp.ErrClosed)
}

// action is the settlement of a handled delivery.
type action int

const (
	actAck     action = iota // acknowledge
	actRequeue               // not settled: requeue for another attempt
	actRetry                 // republish with an incremented redelivery counter
	actDead                  // reject without requeueing: dead letter
)

// decide maps the outcome of a handler call to the settlement of the
// delivery, given how many times it was already redelivered.
func decide(ctx context.Context, err error, redeliveries, maxRedeliveries int) action {
	switch {
	case err == nil:
		return actAck
	case interrupted(ctx, err):
		return actRequeue
	case !errorx.IsRetryable(err):
		// An explicit rejection is settled even while stopping.
		return actDead
	case ctx.Err() != nil:
		return actRequeue
	case redeliveries >= maxRedeliveries:
		return actDead
	default:
		return actRetry
	}
}

// handle processes d with h and settles it. Acknowledgement failures are not
// reported: they mean that the channel is closed, which ends the deliveries.
func (s *subscription) handle(ctx context.Context, h endpoint.Endpoint, d amqp.Delivery) error {
	msg := toMessage(d)
	count := redeliveries(d.Headers)
	_, err := h(ctx, msg)
	switch decide(ctx, err, count, s.b.opts.maxRedeliveries) {
	case actAck:
		_ = d.Ack(false)
	case actRequeue:
		_ = d.Nack(false, true)
	case actDead:
		_ = d.Nack(false, false)
	case actRetry:
		if fn := s.b.opts.backoff; fn != nil && !sleep(ctx, fn(count+1)) {
			_ = d.Nack(false, true)
			return nil
		}
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
		defer cancel()
		if err := s.b.publish(pctx, "redeliver", "", s.queue, redelivery(d, msg.Topic, count+1)); err != nil {
			_ = d.Nack(false, true)
			// A failed retry must not turn an intentional shutdown into a
			// subscription error.
			if ctx.Err() != nil || s.b.ctx.Err() != nil {
				return nil
			}
			return err
		}
		_ = d.Ack(false)
	}
	return nil
}

// interrupted reports whether err means that handling was cut short by the end
// of the subscription rather than settled by the handler.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) == errorx.Canceled)
}

// sleep waits for d or until ctx ends, reporting whether the full delay
// elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
