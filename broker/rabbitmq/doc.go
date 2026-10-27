// Package rabbitmq implements broker.Publisher and broker.ReadySubscriber on
// top of RabbitMQ (AMQP 0-9-1). SubscribeReady reports readiness once the
// queue is declared and bound and the server accepted the consumer.
//
// Messages are published to a durable topic exchange with the topic as the
// routing key. A non-empty group consumes from the durable queue
// QueueName(topic, group), shared by the members of the group; an empty group
// consumes from an exclusive, server-named queue that lives as long as the
// subscription. Message.Header maps to the AMQP headers, Message.Key travels
// in the HeaderKey header.
//
// A message rejected with a retryable error is republished to its queue with
// an incremented HeaderRedeliveries counter and then acknowledged; once the
// counter reaches the configured maximum, or when the error is not retryable,
// the message is rejected without requeueing so that RabbitMQ moves it to the
// direct dead letter exchange with the routing key QueueName(topic, group).
// The dead letters of a group queue are routed into the queue
// DeadLetterQueueName(topic, group); those of a broadcast subscription, routed
// with QueueName(topic, ""), are dropped unless the application binds a queue
// to that key.
package rabbitmq
