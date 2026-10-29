// Package memory provides an in-process broker.Publisher and
// broker.Subscriber intended for tests, examples and single-process
// applications.
//
// Every non-empty consumer group of a topic receives one copy of each
// message, distributed round-robin among the group's subscribers; every
// subscriber with an empty group receives its own copy. Messages are buffered
// in bounded per-subscriber queues, so Publish does not wait for handlers to
// finish but blocks, bounded by its context, while a destination queue is
// full. Nothing is persisted.
package memory
