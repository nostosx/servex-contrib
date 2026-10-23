package rabbitmq

import "time"

const (
	defaultExchange        = "servex"
	defaultMaxRedeliveries = 3
	defaultPrefetch        = 1
	// settleTimeout bounds republishing a message for redelivery, which runs
	// detached from the subscription context.
	settleTimeout = 10 * time.Second
)

// Option configures a Broker.
type Option func(*options)

type options struct {
	exchange           string
	deadLetterExchange string
	maxRedeliveries    int
	backoff            func(attempt int) time.Duration
	prefetch           int
	subscribed         func(topic, group string) // test hook, see withSubscribed
}

// withSubscribed sets a function called once a Subscribe call started
// consuming; it exists for tests.
func withSubscribed(fn func(topic, group string)) Option {
	return func(o *options) { o.subscribed = fn }
}

// WithExchange sets the topic exchange messages are published to. The
// default is "servex".
func WithExchange(name string) Option {
	return func(o *options) { o.exchange = name }
}

// WithDeadLetterExchange sets the direct exchange that receives dead letters,
// routed by QueueName(topic, group). The default is the exchange name followed
// by ".dlx"; it must differ from the exchange messages are published to.
func WithDeadLetterExchange(name string) Option {
	return func(o *options) { o.deadLetterExchange = name }
}

// WithMaxRedeliveries sets how many times a message rejected with a retryable
// error is redelivered before it is dead lettered. The default is 3; negative
// values are treated as 0.
func WithMaxRedeliveries(n int) Option {
	return func(o *options) { o.maxRedeliveries = max(n, 0) }
}

// WithBackoff sets the delay before redelivery number attempt (starting at 1)
// of a message rejected with a retryable error. The subscriber waits, bounded
// by the subscription context, before it republishes the message. By default
// messages are republished immediately; a nil fn restores the default.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(o *options) { o.backoff = fn }
}

// WithPrefetch sets how many unacknowledged messages the server delivers to
// one subscriber ahead of the handler. The default is 1; values below 1 are
// treated as 1.
func WithPrefetch(n int) Option {
	return func(o *options) { o.prefetch = max(n, 1) }
}
