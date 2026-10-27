// Package kafka implements broker.Publisher and broker.ReadySubscriber on top
// of Apache Kafka through the franz-go client. SubscribeReady reports a group
// subscription ready after its first group assignment and a broadcast
// subscription once the cluster answered a ping.
//
// A non-empty group maps to a Kafka consumer group whose offsets are committed
// only after the handler settled the records; an empty group consumes every
// partition of the topic directly, so every broadcast subscriber receives
// every message published after it subscribed. Message.Key maps to the record
// key and Message.Header to the record headers.
//
// Kafka has no per-message negative acknowledgement, so a record rejected with
// a retryable error is redelivered to the handler in-process, up to a
// configurable number of times; records rejected with a non-retryable error or
// that ran out of redeliveries are produced to an optional dead letter topic
// and then committed.
package kafka
