package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
)

// subscription is one active Subscribe call.
type subscription struct {
	b         *Broker
	stream    string
	consumer  jetstream.Consumer
	ephemeral bool
	failures  failureStore
}

// newSubscription resolves the stream of topic and creates the consumer of
// group.
func (b *Broker) newSubscription(ctx context.Context, topic, group string) (*subscription, error) {
	stream := b.opts.stream
	if stream == "" {
		name, err := b.js.StreamNameBySubject(ctx, topic)
		if err != nil {
			return nil, classify(ctx, "subscribe: find stream", err)
		}
		stream = name
	}
	cfg := jetstream.ConsumerConfig{
		FilterSubject: topic,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.opts.ackWait,
		// Unlimited: deliveries that never reached a handler decision must
		// not use up redeliveries, and a server-side limit would stop
		// delivering a message without terminating or dead lettering it.
		// The subscription enforces maxRedeliveries itself.
		MaxDeliver: -1,
	}
	var (
		cons jetstream.Consumer
		err  error
	)
	if group != "" {
		cfg.Durable = DurableName(topic, group)
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
		cons, err = b.js.CreateOrUpdateConsumer(ctx, stream, cfg)
	} else {
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
		cfg.InactiveThreshold = defaultInactiveThreshold
		cons, err = b.js.CreateConsumer(ctx, stream, cfg)
	}
	if err != nil {
		return nil, classify(ctx, "subscribe: create consumer", err)
	}
	sub := &subscription{b: b, stream: stream, consumer: cons, ephemeral: group == ""}
	if sub.ephemeral {
		sub.failures = memoryFailures{}
	} else {
		sub.failures = newBucketFailures(b.bucket, stream, cfg.Durable)
	}
	return sub, nil
}

// run consumes messages until ctx ends or the iterator fails.
func (s *subscription) run(ctx context.Context, h endpoint.Endpoint) error {
	// One message at a time: buffered messages would wait for the handler
	// while their ack deadline runs.
	iter, err := s.consumer.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		return classify(ctx, "subscribe: consume", err)
	}
	defer iter.Stop()
	for {
		m, err := iter.Next(jetstream.NextContext(ctx))
		if err != nil {
			switch {
			case ctx.Err() != nil:
				s.drain(iter)
				return nil
			case errors.Is(err, jetstream.ErrNoHeartbeat):
				// The client is reconnecting; it gives up by closing the
				// connection, which ends the iterator.
				continue
			}
			return classify(context.Background(), "subscribe", err)
		}
		s.handle(ctx, h, m)
		if ctx.Err() != nil {
			s.drain(iter)
			return nil
		}
	}
}

// handle processes m with h and settles it.
func (s *subscription) handle(ctx context.Context, h endpoint.Endpoint, m jetstream.Msg) {
	md, mdErr := m.Metadata()
	if mdErr != nil {
		md = nil
	}
	msg := toMessage(m)

	stop := s.keepAlive(m)
	_, err := h(ctx, msg)
	stop()

	switch {
	case err == nil:
		_ = m.Ack()
		s.clearFailures(ctx, md)
	case interrupted(ctx, err):
		// Not settled: hand it to another subscriber right away. The
		// delivery does not count as a failure.
		_ = m.Nak()
	case !errorx.IsRetryable(err):
		// An explicit rejection is settled even while stopping.
		s.deadLetter(ctx, m, md, msg, err)
	case ctx.Err() != nil:
		_ = m.Nak()
	default:
		s.retry(ctx, m, md, msg, err)
	}
}

// retry records a retryable failure of m and either schedules its
// redelivery or, once it ran out of redeliveries, dead letters it.
func (s *subscription) retry(ctx context.Context, m jetstream.Msg, md *jetstream.MsgMetadata, msg *broker.Message, err error) {
	opts := s.b.opts
	failures := s.countFailure(ctx, md)
	if failures > opts.maxRedeliveries {
		s.deadLetter(ctx, m, md, msg, err)
		return
	}
	_ = m.NakWithDelay(max(opts.backoff(failures), 0))
}

// countFailure records one more retryable failure of the message described by
// md and returns the number of failures so far. If the failure store cannot be
// used, it returns the number of deliveries instead, which is never lower; if
// md is nil, the message cannot be tracked and its redeliveries count as used
// up.
func (s *subscription) countFailure(ctx context.Context, md *jetstream.MsgMetadata) int {
	if md == nil {
		return s.b.opts.maxRedeliveries + 1
	}
	// Detached from the subscription: a Stop during the lookup must not fall
	// back to NumDelivered, which also counts interrupted deliveries and
	// could dead letter the message.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	fallback := int(md.NumDelivered)
	n, err := s.failures.get(ctx, md.Sequence.Stream)
	if err != nil {
		return fallback
	}
	n++
	if n > s.b.opts.maxRedeliveries {
		return n
	}
	if err := s.failures.set(ctx, md.Sequence.Stream, n); err != nil {
		return max(n, fallback)
	}
	return n
}

// clearFailures removes the failure count of a settled message. Only a
// redelivered message can have one.
func (s *subscription) clearFailures(ctx context.Context, md *jetstream.MsgMetadata) {
	if md == nil || md.NumDelivered <= 1 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if n, err := s.failures.get(ctx, md.Sequence.Stream); err == nil && n > 0 {
		// A leftover counter expires with the bucket's TTL.
		_ = s.failures.clear(ctx, md.Sequence.Stream)
	}
}

// deadLetter hands msg to the dead letter function and terminates m.
func (s *subscription) deadLetter(ctx context.Context, m jetstream.Msg, md *jetstream.MsgMetadata, msg *broker.Message, err error) {
	if fn := s.b.opts.deadLetter; fn != nil {
		fn(msg, err)
	}
	_ = m.TermWithReason(err.Error())
	s.clearFailures(ctx, md)
}

// keepAlive extends the ack deadline of m while its handler runs and returns
// a function that stops doing so.
func (s *subscription) keepAlive(m jetstream.Msg) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(max(s.b.opts.ackWait/2, time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = m.InProgress()
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

// drain stops pulling and negatively acknowledges buffered messages so that
// they are redelivered without waiting for their ack deadline.
func (s *subscription) drain(iter jetstream.MessagesContext) {
	iter.Drain()
	for {
		m, err := iter.Next(jetstream.NextMaxWait(drainTimeout))
		if err != nil {
			return
		}
		_ = m.Nak()
	}
}

// close deletes an ephemeral consumer.
func (s *subscription) close(ctx context.Context) {
	if !s.ephemeral {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	_ = s.b.js.DeleteConsumer(ctx, s.stream, s.consumer.CachedInfo().Name)
}

// interrupted reports whether err means that handling was cut short by the end
// of the subscription rather than settled by the handler.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) == errorx.Canceled)
}
