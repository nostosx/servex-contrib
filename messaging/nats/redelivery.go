package nats

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultRedeliveryBucket is the default name of the key-value bucket that
// records how often the handlers of a group rejected a message with a
// retryable error. See WithRedeliveryBucket.
const DefaultRedeliveryBucket = "SERVEX_REDELIVERIES"

// failureStore records, per stream sequence, how many times handlers rejected
// a message with a retryable error. Deliveries that never reached a handler
// decision are not recorded, so they do not use up redeliveries.
type failureStore interface {
	// get returns the recorded number of failures of seq, 0 if none.
	get(ctx context.Context, seq uint64) (int, error)
	set(ctx context.Context, seq uint64, n int) error
	clear(ctx context.Context, seq uint64) error
}

// memoryFailures is the failure store of an ephemeral consumer, which only
// its own subscription consumes. It is used by one goroutine at a time.
type memoryFailures map[uint64]int

func (m memoryFailures) get(_ context.Context, seq uint64) (int, error) { return m[seq], nil }

func (m memoryFailures) set(_ context.Context, seq uint64, n int) error {
	m[seq] = n
	return nil
}

func (m memoryFailures) clear(_ context.Context, seq uint64) error {
	delete(m, seq)
	return nil
}

// bucketFailures is the failure store of a durable consumer, shared by the
// members of its group through a JetStream key-value bucket.
type bucketFailures struct {
	bucket *redeliveryBucket
	prefix string // encoded stream and consumer names followed by '.'
}

func newBucketFailures(bucket *redeliveryBucket, stream, consumer string) *bucketFailures {
	enc := base64.RawURLEncoding
	return &bucketFailures{bucket: bucket, prefix: enc.EncodeToString([]byte(stream)) + "." + enc.EncodeToString([]byte(consumer)) + "."}
}

func (b *bucketFailures) key(seq uint64) string {
	return b.prefix + strconv.FormatUint(seq, 10)
}

func (b *bucketFailures) get(ctx context.Context, seq uint64) (int, error) {
	kv, err := b.bucket.open(ctx, false)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	e, err := kv.Get(ctx, b.key(seq))
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return 0, nil
	case err != nil:
		return 0, err
	}
	return strconv.Atoi(string(e.Value()))
}

func (b *bucketFailures) set(ctx context.Context, seq uint64, n int) error {
	kv, err := b.bucket.open(ctx, true)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, b.key(seq), strconv.AppendInt(nil, int64(n), 10))
	return err
}

func (b *bucketFailures) clear(ctx context.Context, seq uint64) error {
	kv, err := b.bucket.open(ctx, false)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return kv.Purge(ctx, b.key(seq))
}

// redeliveryBucket opens the key-value bucket of a Broker lazily: it is only
// created once a handler of a group first fails with a retryable error.
type redeliveryBucket struct {
	js   jetstream.JetStream
	name string

	mu sync.Mutex
	kv jetstream.KeyValue
}

// open returns the bucket, creating it if create is set and it does not exist.
func (r *redeliveryBucket) open(ctx context.Context, create bool) (jetstream.KeyValue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.kv != nil {
		return r.kv, nil
	}
	kv, err := r.js.KeyValue(ctx, r.name)
	if errors.Is(err, jetstream.ErrBucketNotFound) && create {
		kv, err = r.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      r.name,
			Description: "Retryable handler failures per message, maintained by servex broker/nats",
			History:     1,
			TTL:         redeliveryTTL,
		})
		if errors.Is(err, jetstream.ErrBucketExists) {
			// Created concurrently, possibly with another configuration.
			kv, err = r.js.KeyValue(ctx, r.name)
		}
	}
	if err != nil {
		return nil, err
	}
	r.kv = kv
	return kv, nil
}
