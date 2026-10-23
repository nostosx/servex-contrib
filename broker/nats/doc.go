// Package nats implements broker.Publisher and broker.Subscriber on top of
// NATS JetStream.
//
// Topics are subjects that must be bound to a JetStream stream managed by the
// application. A non-empty group maps to a durable pull consumer shared by the
// members of the group; an empty group creates an ephemeral consumer per
// subscriber that receives the messages published after it subscribed.
// Message.Header maps to the NATS message headers and Message.Key travels in
// the HeaderKey header.
//
// A message rejected with a retryable error is negatively acknowledged with a
// redelivery delay. Once the redelivery limit is exhausted, or when the error
// is not retryable, the optional dead letter function runs before the message
// is terminated. Group members share handler-rejection counts through a
// key-value bucket; interrupted deliveries do not count while that bucket is
// available. If it cannot be used, the fallback counts server deliveries,
// including interrupted ones (see WithRedeliveryBucket). Broadcast subscribers
// count handler rejections in memory.
package nats
