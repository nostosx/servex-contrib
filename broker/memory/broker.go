package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
)

// errClosed is returned by Publish once the Broker is closed.
var errClosed = errors.New("memory: broker is closed")

// Broker is an in-memory implementation of broker.Publisher,
// broker.Subscriber and broker.ReadySubscriber. Its methods are safe for concurrent use. The zero value
// is not usable; create one with New.
//
// A message rejected with a retryable error is redelivered to the same
// subscriber immediately, up to the configured number of redeliveries. When a
// group subscriber leaves, its queued messages and a message whose handling
// was interrupted are handed to the remaining members of the group; queued
// messages of a leaving broadcast subscriber are discarded.
type Broker struct {
	opts   options
	ctx    context.Context // canceled by Close
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	topics map[string]*topicSubs
}

// topicSubs holds the subscribers of one topic.
type topicSubs struct {
	groups    map[string]*group
	broadcast []*subscriber
}

// group is a non-empty consumer group of a topic.
type group struct {
	members []*subscriber
	next    int
}

// subscriber is one active Subscribe call.
type subscriber struct {
	group string
	queue chan *broker.Message
	gone  chan struct{} // closed when the subscriber leaves

	// mu is held for reading by publishers while they offer a message and for
	// writing by the subscriber while it leaves, so that no message can be
	// queued after the final drain.
	mu   sync.RWMutex
	left bool
}

// New creates a Broker.
func New(opts ...Option) *Broker {
	o := options{maxRedeliveries: defaultMaxRedeliveries, queueSize: defaultQueueSize}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{opts: o, ctx: ctx, cancel: cancel, topics: make(map[string]*topicSubs)}
}

// Publish delivers a copy of msg to every consumer group and broadcast
// subscriber of msg.Topic. Messages of a topic without subscribers are
// dropped. Publish returns once the copies are queued; while a destination
// queue is full it blocks until there is room, ctx ends or the Broker is
// closed. If ctx ends part-way, some destinations may already hold the
// message.
func (b *Broker) Publish(ctx context.Context, msg *broker.Message) error {
	switch {
	case msg == nil:
		return errors.New("memory: publish: nil message")
	case msg.Topic == "":
		return errors.New("memory: publish: empty topic")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory: publish: %w", err)
	}

	type target struct {
		group string
		sub   *subscriber
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errClosed
	}
	var targets []target
	if t := b.topics[msg.Topic]; t != nil {
		for name := range t.groups {
			targets = append(targets, target{name, b.pickLocked(msg.Topic, name)})
		}
		for _, s := range t.broadcast {
			targets = append(targets, target{"", s})
		}
	}
	b.mu.Unlock()

	for _, t := range targets {
		if err := b.deliver(ctx, msg.Topic, t.group, t.sub, cloneMessage(msg)); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe consumes topic as a member of group until ctx ends or the Broker
// is closed, returning nil in both cases. It returns nil immediately if the
// Broker is already closed. The context passed to h is canceled when ctx ends
// or the Broker is closed.
func (b *Broker) Subscribe(ctx context.Context, topic, group string, h endpoint.Endpoint) error {
	return b.SubscribeReady(ctx, topic, group, h, nil)
}

// SubscribeReady behaves like Subscribe and implements
// broker.ReadySubscriber: it calls ready once the subscriber has joined
// topic, so messages published afterwards reach it. A nil ready is allowed.
// It returns without calling ready when the arguments are invalid or the
// Broker is closed.
func (b *Broker) SubscribeReady(ctx context.Context, topic, group string, h endpoint.Endpoint, ready func()) error {
	switch {
	case topic == "":
		return errors.New("memory: subscribe: empty topic")
	case h == nil:
		return errors.New("memory: subscribe: nil handler")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()

	s := &subscriber{
		group: group,
		queue: make(chan *broker.Message, b.opts.queueSize),
		gone:  make(chan struct{}),
	}
	if !b.join(topic, s) {
		return nil
	}
	if ready != nil {
		ready()
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case msg := <-s.queue:
			if !b.handle(ctx, h, msg) {
				b.leave(topic, s, msg)
				return nil
			}
		}
	}
	b.leave(topic, s, nil)
	return nil
}

// Close closes the Broker. Afterwards Publish returns an error, blocked
// publishers are released with an error, active Subscribe calls return nil
// and queued messages are discarded. Close does not wait for handlers to
// finish. It is idempotent and always returns nil.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.cancel()
	}
	return nil
}

// handle processes msg with h, redelivering it on retryable errors. It
// reports false when the subscription ended before msg was settled.
func (b *Broker) handle(ctx context.Context, h endpoint.Endpoint, msg *broker.Message) bool {
	var err error
	for range b.opts.maxRedeliveries + 1 {
		if _, err = h(ctx, msg); err == nil {
			return true
		}
		if interrupted(ctx, err) {
			return false
		}
		if !errorx.IsRetryable(err) {
			// An explicit rejection is settled even while stopping.
			break
		}
		if ctx.Err() != nil {
			return false
		}
	}
	if b.opts.deadLetter != nil {
		b.opts.deadLetter(msg, err)
	}
	return true
}

// interrupted reports whether err means that handling was cut short by the end
// of the subscription rather than settled by the handler.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) == errorx.Canceled)
}

// join registers s for topic. It reports false if the Broker is closed.
func (b *Broker) join(topic string, s *subscriber) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	t := b.topics[topic]
	if t == nil {
		t = &topicSubs{groups: make(map[string]*group)}
		b.topics[topic] = t
	}
	if s.group == "" {
		t.broadcast = append(t.broadcast, s)
		return true
	}
	g := t.groups[s.group]
	if g == nil {
		g = &group{}
		t.groups[s.group] = g
	}
	g.members = append(g.members, s)
	return true
}

// leave unregisters s and hands its unsettled messages, starting with
// inflight if non-nil, to the remaining members of its group. The hand-off
// runs in the background so that Subscribe returns promptly even when the
// remaining members are backlogged; it ends once the messages are queued, the
// group is empty or the Broker is closed.
func (b *Broker) leave(topic string, s *subscriber, inflight *broker.Message) {
	b.mu.Lock()
	if t := b.topics[topic]; t != nil {
		isSelf := func(o *subscriber) bool { return o == s }
		if s.group == "" {
			t.broadcast = slices.DeleteFunc(t.broadcast, isSelf)
		} else if g := t.groups[s.group]; g != nil {
			g.members = slices.DeleteFunc(g.members, isSelf)
			if len(g.members) == 0 {
				delete(t.groups, s.group)
			}
		}
		if len(t.groups) == 0 && len(t.broadcast) == 0 {
			delete(b.topics, topic)
		}
	}
	close(s.gone)
	closed := b.closed
	b.mu.Unlock()

	// Wait for in-flight offers to finish, then take whatever they queued.
	s.mu.Lock()
	s.left = true
	var pending []*broker.Message
	if inflight != nil {
		pending = append(pending, inflight)
	}
	for len(s.queue) > 0 {
		pending = append(pending, <-s.queue)
	}
	s.mu.Unlock()

	if closed || s.group == "" || len(pending) == 0 {
		return
	}
	go b.handOff(topic, s.group, pending)
}

// handOff queues pending at the remaining members of group, blocking on full
// queues until the Broker is closed.
func (b *Broker) handOff(topic, group string, pending []*broker.Message) {
	for _, msg := range pending {
		b.mu.Lock()
		next := b.pickLocked(topic, group)
		b.mu.Unlock()
		if next == nil {
			return
		}
		if b.deliver(b.ctx, topic, group, next, msg) != nil {
			return
		}
	}
}

// pickLocked returns the next member of group in round-robin order, or nil if
// the group has no members or the Broker is closed. b.mu must be held.
func (b *Broker) pickLocked(topic, name string) *subscriber {
	if b.closed {
		return nil
	}
	t := b.topics[topic]
	if t == nil {
		return nil
	}
	g := t.groups[name]
	if g == nil || len(g.members) == 0 {
		return nil
	}
	s := g.members[g.next%len(g.members)]
	g.next = (g.next + 1) % len(g.members)
	return s
}

// deliver queues msg at s. If s leaves meanwhile, a group message moves on to
// the next member of the group and a broadcast message is dropped.
func (b *Broker) deliver(ctx context.Context, topic, group string, s *subscriber, msg *broker.Message) error {
	for s != nil {
		ok, err := b.offer(ctx, s, msg)
		if err != nil || ok {
			return err
		}
		if group == "" {
			return nil
		}
		b.mu.Lock()
		s = b.pickLocked(topic, group)
		b.mu.Unlock()
	}
	return nil
}

// offer tries to queue msg at s. It reports false without an error if s has
// left.
func (b *Broker) offer(ctx context.Context, s *subscriber, msg *broker.Message) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.left {
		return false, nil
	}
	select {
	case s.queue <- msg:
		return true, nil
	default:
	}
	select {
	case s.queue <- msg:
		return true, nil
	case <-s.gone:
		return false, nil
	case <-ctx.Done():
		if b.ctx.Err() != nil {
			return false, errClosed
		}
		return false, fmt.Errorf("memory: publish: %w", ctx.Err())
	case <-b.ctx.Done():
		return false, errClosed
	}
}

// cloneMessage returns a deep copy of msg so that publishers and subscribers
// never share mutable state.
func cloneMessage(msg *broker.Message) *broker.Message {
	return &broker.Message{
		Topic:  msg.Topic,
		Key:    msg.Key,
		Header: msg.Header.Clone(),
		Body:   slices.Clone(msg.Body),
	}
}
