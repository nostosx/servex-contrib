package nats

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeKV is an in-memory key-value bucket. Methods that the failure store
// does not use panic through the nil embedded interface.
type fakeKV struct {
	jetstream.KeyValue
	mu      sync.Mutex
	entries map[string][]byte
	err     error // returned by every operation if set
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	value []byte
}

func (e fakeEntry) Value() []byte { return e.value }

func (kv *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.err != nil {
		return nil, kv.err
	}
	v, ok := kv.entries[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return fakeEntry{value: v}, nil
}

func (kv *fakeKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.err != nil {
		return 0, kv.err
	}
	kv.entries[key] = value
	return uint64(len(kv.entries)), nil
}

func (kv *fakeKV) Purge(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.err != nil {
		return kv.err
	}
	delete(kv.entries, key)
	return nil
}

// fakeKVJetStream serves the key-value buckets of a failure store.
type fakeKVJetStream struct {
	jetstream.JetStream
	kv        *fakeKV // the existing bucket, nil if there is none
	lookupErr error   // returned by KeyValue instead of the lookup result
	createErr error   // returned by CreateKeyValue
	lookups   int
	creates   []jetstream.KeyValueConfig
}

func (js *fakeKVJetStream) KeyValue(_ context.Context, _ string) (jetstream.KeyValue, error) {
	js.lookups++
	switch {
	case js.lookupErr != nil:
		return nil, js.lookupErr
	case js.kv == nil:
		return nil, jetstream.ErrBucketNotFound
	}
	return js.kv, nil
}

func (js *fakeKVJetStream) CreateKeyValue(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	js.creates = append(js.creates, cfg)
	if js.createErr != nil {
		return nil, js.createErr
	}
	js.kv = &fakeKV{entries: map[string][]byte{}}
	return js.kv, nil
}

func newFakeFailures(js *fakeKVJetStream) *bucketFailures {
	return newBucketFailures(&redeliveryBucket{js: js, name: "COUNTERS"}, "EVENTS", "g_orders")
}

func TestBucketFailuresWithoutBucket(t *testing.T) {
	js := &fakeKVJetStream{}
	f := newFakeFailures(js)
	// Reading and clearing do not create the bucket.
	if n, err := f.get(t.Context(), 7); n != 0 || err != nil {
		t.Fatalf("get() = %d, %v, want 0, nil", n, err)
	}
	if err := f.clear(t.Context(), 7); err != nil {
		t.Fatalf("clear() = %v, want nil", err)
	}
	if len(js.creates) != 0 {
		t.Fatalf("bucket created by get or clear: %+v", js.creates)
	}
	// Recording a failure creates it.
	if err := f.set(t.Context(), 7, 2); err != nil {
		t.Fatalf("set() = %v", err)
	}
	if len(js.creates) != 1 || js.creates[0].Bucket != "COUNTERS" || js.creates[0].TTL != redeliveryTTL ||
		js.creates[0].History != 1 {
		t.Fatalf("created buckets = %+v", js.creates)
	}
	if n, err := f.get(t.Context(), 7); n != 2 || err != nil {
		t.Fatalf("get() after set = %d, %v, want 2, nil", n, err)
	}
	if err := f.clear(t.Context(), 7); err != nil {
		t.Fatalf("clear() = %v", err)
	}
	if n, err := f.get(t.Context(), 7); n != 0 || err != nil {
		t.Fatalf("get() after clear = %d, %v, want 0, nil", n, err)
	}
}

func TestBucketFailuresKeys(t *testing.T) {
	kv := &fakeKV{entries: map[string][]byte{}}
	js := &fakeKVJetStream{kv: kv}
	a := newBucketFailures(&redeliveryBucket{js: js, name: "COUNTERS"}, "EVENTS", "a")
	b := newBucketFailures(&redeliveryBucket{js: js, name: "COUNTERS"}, "EVENTS", "b")
	if err := a.set(t.Context(), 1, 3); err != nil {
		t.Fatal(err)
	}
	// Consumers of the same stream do not share counters.
	if n, err := b.get(t.Context(), 1); n != 0 || err != nil {
		t.Fatalf("other consumer get() = %d, %v, want 0, nil", n, err)
	}
	// Names are encoded, so a dot in a name cannot forge another key.
	if got, want := a.key(42), "RVZFTlRT.YQ.42"; got != want {
		t.Fatalf("key() = %q, want %q", got, want)
	}
	// The bucket is looked up once and then reused.
	if js.lookups != 2 {
		t.Fatalf("bucket lookups = %d, want one per store", js.lookups)
	}
}

func TestBucketFailuresErrors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		js   *fakeKVJetStream
	}{
		{"bucket lookup fails", &fakeKVJetStream{lookupErr: boom}},
		{"bucket operation fails", &fakeKVJetStream{kv: &fakeKV{entries: map[string][]byte{}, err: boom}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeFailures(tt.js)
			if _, err := f.get(t.Context(), 1); !errors.Is(err, boom) {
				t.Errorf("get() = %v, want %v", err, boom)
			}
			if err := f.set(t.Context(), 1, 1); !errors.Is(err, boom) {
				t.Errorf("set() = %v, want %v", err, boom)
			}
			if err := f.clear(t.Context(), 1); !errors.Is(err, boom) {
				t.Errorf("clear() = %v, want %v", err, boom)
			}
			if len(tt.js.creates) != 0 {
				t.Errorf("bucket created after a failed lookup: %+v", tt.js.creates)
			}
		})
	}
}

func TestBucketFailuresCorruptCounter(t *testing.T) {
	kv := &fakeKV{entries: map[string][]byte{}}
	f := newFakeFailures(&fakeKVJetStream{kv: kv})
	kv.entries[f.key(5)] = []byte("not a number")
	if _, err := f.get(t.Context(), 5); err == nil {
		t.Fatal("get() of a corrupt counter = nil error")
	}
}

func TestRedeliveryBucketCreatedConcurrently(t *testing.T) {
	existing := &fakeKV{entries: map[string][]byte{}}
	js := &fakeKVJetStream{createErr: jetstream.ErrBucketExists}
	// Another process creates the bucket between the lookup and the create
	// call: the bucket is looked up again and used as is.
	r := &redeliveryBucket{js: &raceJS{fakeKVJetStream: js, created: existing}, name: "COUNTERS"}
	kv, err := r.open(t.Context(), true)
	if err != nil {
		t.Fatalf("open() = %v", err)
	}
	if kv != existing {
		t.Fatalf("open() = %v, want the concurrently created bucket", kv)
	}
	if again, err := r.open(t.Context(), false); err != nil || again != existing {
		t.Fatalf("second open() = %v, %v, want the cached bucket", again, err)
	}
	if js.lookups != 2 {
		t.Fatalf("bucket lookups = %d, want 2", js.lookups)
	}

	// A failed create is reported.
	js = &fakeKVJetStream{createErr: errors.New("insufficient resources")}
	r = &redeliveryBucket{js: js, name: "COUNTERS"}
	if _, err := r.open(t.Context(), true); !errors.Is(err, js.createErr) {
		t.Fatalf("open() = %v, want %v", err, js.createErr)
	}
}

// raceJS makes the bucket exist once CreateKeyValue was called.
type raceJS struct {
	*fakeKVJetStream
	created *fakeKV
}

func (js *raceJS) CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	kv, err := js.fakeKVJetStream.CreateKeyValue(ctx, cfg)
	js.kv = js.created
	return kv, err
}
