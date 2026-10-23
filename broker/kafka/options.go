package kafka

import "time"

const (
	defaultMaxRedeliveries = 3
	defaultPingInterval    = 10 * time.Second
	// maxPingFailures is the number of consecutive failed liveness pings
	// after which a subscription reports the connection as lost.
	maxPingFailures = 3
	// settleTimeout bounds dead letter production and offset commits, which
	// run detached from the subscription context so that a message settled
	// while stopping is still recorded.
	settleTimeout = 10 * time.Second
	// leaveTimeout bounds leaving the consumer group when a subscription
	// ends.
	leaveTimeout = 5 * time.Second
)

// Option configures a Broker.
type Option func(*options)

type options struct {
	maxRedeliveries int
	backoff         func(attempt int) time.Duration
	deadLetterTopic string
	pingInterval    time.Duration
	subscribed      func(topic, group string) // test hook, see withSubscribed
}

// withSubscribed sets a function called once a Subscribe call created its
// consumer client; it exists for tests.
func withSubscribed(fn func(topic, group string)) Option {
	return func(o *options) { o.subscribed = fn }
}

// WithMaxRedeliveries sets how many times a record rejected with a retryable
// error is redelivered to the handler before it is dead lettered. The default
// is 3; negative values are treated as 0.
func WithMaxRedeliveries(n int) Option {
	return func(o *options) { o.maxRedeliveries = max(n, 0) }
}

// WithBackoff sets the delay before redelivery number attempt
// (starting at 1) of a record rejected with a retryable error. The wait is
// bounded by the subscription context. By default records are redelivered
// immediately; a nil fn restores the default.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(o *options) { o.backoff = fn }
}

// WithDeadLetterTopic sets the topic that receives records which were
// rejected with a non-retryable error or ran out of redeliveries. The dead
// letter record keeps the key, value and headers of the original record and
// adds the HeaderOriginalTopic and HeaderError headers. By default such
// records are dropped.
func WithDeadLetterTopic(topic string) Option {
	return func(o *options) { o.deadLetterTopic = topic }
}

// WithPingInterval sets how often a subscription checks that the cluster is
// reachable. After three consecutive failed checks Subscribe returns a
// retryable errorx.Unavailable error. The default is 10s; values <= 0
// disable the check.
func WithPingInterval(d time.Duration) Option {
	return func(o *options) { o.pingInterval = d }
}
