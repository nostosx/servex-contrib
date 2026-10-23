package nats

import (
	"time"

	"github.com/nostosx/servex/messaging/broker"
)

const (
	defaultMaxRedeliveries   = 3
	defaultAckWait           = 30 * time.Second
	defaultInactiveThreshold = time.Minute
	backoffBase              = 100 * time.Millisecond
	backoffCap               = 10 * time.Second
	// settleTimeout bounds cleanup requests that run detached from the
	// subscription context, such as deleting an ephemeral consumer.
	settleTimeout = 5 * time.Second
	// drainTimeout bounds waiting for buffered messages when a subscription
	// ends.
	drainTimeout = time.Second
	// redeliveryTTL is the age after which a failure counter of a group
	// expires. An expired counter only grants a message more redeliveries.
	redeliveryTTL = 24 * time.Hour
)

// Option configures a Broker.
type Option func(*options)

type options struct {
	maxRedeliveries int
	backoff         func(attempt int) time.Duration
	deadLetter      func(msg *broker.Message, err error)
	stream          string
	ackWait         time.Duration
	bucket          string
	subscribed      func(topic, group string) // test hook, see withSubscribed
}

// WithMaxRedeliveries sets how many times a message rejected with a retryable
// error is redelivered before it is dead lettered and terminated. Only handler
// rejections count unless the shared counter is unavailable, in which case
// server deliveries are counted (see WithRedeliveryBucket). Consumers have no
// MaxDeliver limit; the subscription enforces the limit. The default is 3;
// negative values are treated as 0.
func WithMaxRedeliveries(n int) Option {
	return func(o *options) { o.maxRedeliveries = max(n, 0) }
}

// WithBackoff sets the redelivery delay requested after retryable failure
// number attempt (starting at 1). The default doubles from 100ms per attempt,
// capped at 10s. A nil fn restores the default.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(o *options) { o.backoff = fn }
}

// WithDeadLetter sets the function that receives messages which were rejected
// with a non-retryable error or ran out of redeliveries, together with the
// last handler error. It is called synchronously from the subscriber before
// the message is terminated. By default such messages are only terminated.
func WithDeadLetter(fn func(msg *broker.Message, err error)) Option {
	return func(o *options) { o.deadLetter = fn }
}

// WithStream sets the stream that holds the subscribed topics. By default the
// stream is looked up by subject on every Subscribe call.
func WithStream(name string) Option {
	return func(o *options) { o.stream = name }
}

// WithAckWait sets how long the server waits for an acknowledgement before it
// redelivers a message. While a handler runs, the subscriber extends the
// deadline periodically. The default is 30s; values <= 0 restore it.
func WithAckWait(d time.Duration) Option {
	return func(o *options) { o.ackWait = d }
}

// WithRedeliveryBucket sets the JetStream key-value bucket in which the
// members of a group record how often their handlers rejected a message with
// a retryable error, so that the count survives redelivery to another member
// or process. The bucket is created, with a TTL of 24 hours, when a handler
// first fails with a retryable error and does not exist yet; an existing
// bucket is used as is. If the bucket cannot be used, the number of
// deliveries reported by the server is counted instead, which includes
// interrupted deliveries. Subscriptions with an empty group count in memory.
// The default is DefaultRedeliveryBucket; an empty name restores it.
func WithRedeliveryBucket(name string) Option {
	return func(o *options) { o.bucket = name }
}

// withSubscribed sets a function called once a Subscribe call created its
// consumer; it exists for tests.
func withSubscribed(fn func(topic, group string)) Option {
	return func(o *options) { o.subscribed = fn }
}

// defaultBackoff doubles from backoffBase per attempt and is capped at
// backoffCap.
func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		return backoffBase
	}
	return min(backoffBase<<min(attempt-1, 8), backoffCap)
}
