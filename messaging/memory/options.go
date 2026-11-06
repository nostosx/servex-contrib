package memory

import "github.com/nostosx/servex/messaging"

const (
	defaultMaxRedeliveries = 3
	defaultQueueSize       = 1024
)

// Option configures a Broker.
type Option func(*options)

type options struct {
	settlement      messaging.SettlementObserver
	maxRedeliveries int
	deadLetter      func(msg *messaging.Message, err error)
	queueSize       int
}

// WithMaxRedeliveries sets how many times a message rejected with a retryable
// error is redelivered before it goes to the dead letter function. The
// default is 3; negative values are treated as 0.
func WithMaxRedeliveries(n int) Option {
	return func(o *options) { o.maxRedeliveries = max(n, 0) }
}

// WithDeadLetter sets the function that receives messages which were rejected
// with a non-retryable error or ran out of redeliveries, together with the
// last handler error. It is called synchronously from the subscriber that
// gave up on the message. By default such messages are dropped.
func WithDeadLetter(fn func(msg *messaging.Message, err error)) Option {
	return func(o *options) { o.deadLetter = fn }
}

// withQueueSize sets the capacity of each subscriber queue; it exists for
// tests.
func withQueueSize(n int) Option {
	return func(o *options) { o.queueSize = max(n, 1) }
}

// WithSettlementObserver observes native settlement actions after processing.
// It must be concurrency-safe and prompt; it never changes settlement policy.
func WithSettlementObserver(observer messaging.SettlementObserver) Option {
	return func(o *options) { o.settlement = observer }
}
