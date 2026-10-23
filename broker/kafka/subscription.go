package kafka

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// subscription is one active Subscribe call with its own consumer client.
type subscription struct {
	b     *Broker
	topic string
	group string
	cl    *kgo.Client
	lost  atomic.Pointer[error] // set when the liveness check gives up
}

// newSubscription creates the consumer client of a Subscribe call.
func (b *Broker) newSubscription(topic, group string) (*subscription, error) {
	kopts := slices.Clone(b.base)
	kopts = append(kopts, kgo.ConsumeTopics(topic))
	if group != "" {
		kopts = append(kopts,
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
		)
	} else {
		// Broadcast: read every partition directly, starting with the records
		// produced from now on.
		kopts = append(kopts, kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().UnixMilli())))
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, invalid("subscribe: new client: %v", err)
	}
	if fn := b.opts.subscribed; fn != nil {
		fn(topic, group)
	}
	return &subscription{b: b, topic: topic, group: group, cl: cl}, nil
}

// run consumes until ctx ends or a fatal error occurs, then closes the
// consumer client. cancel cancels ctx.
func (s *subscription) run(ctx context.Context, cancel context.CancelFunc, h endpoint.Endpoint) error {
	defer s.close()
	watchDone := make(chan struct{})
	defer func() { <-watchDone }()
	defer cancel()
	go func() {
		defer close(watchDone)
		s.watch(ctx, cancel)
	}()

	for {
		fetches := s.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			s.allowRebalance()
			return s.endError()
		}
		if err := fetchError(fetches); err != nil {
			s.allowRebalance()
			return err
		}
		settled, err := s.process(ctx, h, fetches)
		commitErr := s.commit(ctx, settled)
		s.allowRebalance()
		switch {
		case err != nil:
			return err
		case commitErr != nil:
			return commitErr
		case ctx.Err() != nil:
			return s.endError()
		}
	}
}

// endError returns the error to report once the subscription context ended:
// the connection loss detected by the liveness check, or nil for a regular
// end.
func (s *subscription) endError() error {
	if err := s.lost.Load(); err != nil {
		return *err
	}
	return nil
}

// process handles the records of fetches in order and returns the records
// that were settled. It stops at the first record that could not be settled
// because the subscription ended, and returns an error if a record could not
// be dead lettered.
func (s *subscription) process(ctx context.Context, h endpoint.Endpoint, fetches kgo.Fetches) ([]*kgo.Record, error) {
	var settled []*kgo.Record
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		if ctx.Err() != nil {
			return settled, nil
		}
		ok, err := s.handle(ctx, h, rec)
		if err != nil || !ok {
			return settled, err
		}
		settled = append(settled, rec)
	}
	return settled, nil
}

// handle processes rec with h, redelivering it on retryable errors. It
// reports false when the subscription ended before rec was settled.
func (s *subscription) handle(ctx context.Context, h endpoint.Endpoint, rec *kgo.Record) (bool, error) {
	msg := toMessage(rec)
	opts := s.b.opts
	var err error
	for attempt := range opts.maxRedeliveries + 1 {
		if attempt > 0 && opts.backoff != nil && !sleep(ctx, opts.backoff(attempt)) {
			return false, nil
		}
		if _, err = h(ctx, msg); err == nil {
			return true, nil
		}
		if interrupted(ctx, err) {
			return false, nil
		}
		if !errorx.IsRetryable(err) {
			// An explicit rejection is settled even while stopping.
			break
		}
		if ctx.Err() != nil {
			return false, nil
		}
	}
	if err := s.deadLetter(ctx, rec, err); err != nil {
		if ctx.Err() != nil {
			// The subscription is ending: leave the record unsettled so that
			// it is redelivered.
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// deadLetter produces rec to the dead letter topic, if one is configured.
func (s *subscription) deadLetter(ctx context.Context, rec *kgo.Record, cause error) error {
	topic := s.b.opts.deadLetterTopic
	if topic == "" {
		return nil
	}
	dl := &kgo.Record{
		Topic:   topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: slices.Clone(rec.Headers),
	}
	dl.Headers = append(dl.Headers,
		kgo.RecordHeader{Key: HeaderOriginalTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: HeaderError, Value: []byte(cause.Error())},
	)
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if err := s.b.producer.ProduceSync(pctx, dl).FirstErr(); err != nil {
		return classify(pctx, "dead letter", err)
	}
	return nil
}

// commit commits the offsets of settled records of a group subscription.
func (s *subscription) commit(ctx context.Context, settled []*kgo.Record) error {
	if s.group == "" || len(settled) == 0 {
		return nil
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	return commitError(s.cl.CommitRecords(cctx, settled...))
}

// allowRebalance releases a rebalance blocked by the last poll.
func (s *subscription) allowRebalance() {
	if s.group != "" {
		s.cl.AllowRebalance()
	}
}

// close leaves the group, bounded by leaveTimeout, and closes the client.
func (s *subscription) close() {
	if s.group != "" {
		ctx, cancel := context.WithTimeout(context.Background(), leaveTimeout)
		_ = s.cl.LeaveGroupContext(ctx)
		cancel()
	}
	s.cl.CloseAllowingRebalance()
}

// watch pings the cluster every ping interval until ctx ends. After
// maxPingFailures consecutive failures it records the connection loss and
// cancels the subscription.
func (s *subscription) watch(ctx context.Context, cancel context.CancelFunc) {
	interval := s.b.opts.pingInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pctx, pcancel := context.WithTimeout(ctx, interval)
		err := s.cl.Ping(pctx)
		pcancel()
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			failures = 0
			continue
		}
		if failures++; failures >= maxPingFailures {
			lost := classify(context.Background(), "subscribe: connection lost", err)
			s.lost.Store(&lost)
			cancel()
			return
		}
	}
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
