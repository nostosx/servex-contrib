package consul_test

import (
	"encoding/json/v2"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

// fakeConsul emulates the subset of the Consul KV HTTP API used by Source:
// GET /v1/kv/<key> with optional recurse, and blocking queries driven by the
// index and wait parameters with the X-Consul-Index response header. Like
// a real server, any KV write wakes every blocking query, so a query may
// return without a change of the keys it reads.
type fakeConsul struct {
	srv *httptest.Server

	mu      sync.Mutex
	index   uint64
	kv      map[string]entry
	changed chan struct{} // closed and replaced on every write
	fail    []int         // status codes returned by the next requests
	queries int           // number of KV requests served
	blocked int           // number of requests currently blocking
	epoch   int           // incremented by setIndex to abandon blocking queries
}

// entry is one stored key.
type entry struct {
	value       []byte
	createIndex uint64
	modifyIndex uint64
}

func newFakeConsul(t *testing.T) *fakeConsul {
	t.Helper()
	f := &fakeConsul{index: 1, kv: map[string]entry{}, changed: make(chan struct{})}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

// client returns a Consul client talking to f.
func (f *fakeConsul) client(t *testing.T) *api.Client {
	t.Helper()
	c, err := api.NewClient(&api.Config{Address: strings.TrimPrefix(f.srv.URL, "http://"), Scheme: "http"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// put stores value under key and returns the new index.
func (f *fakeConsul) put(key, value string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index++
	e, ok := f.kv[key]
	if !ok {
		e.createIndex = f.index
	}
	e.value, e.modifyIndex = []byte(value), f.index
	f.kv[key] = e
	f.wake()
	return f.index
}

// folder stores a key without a value, as the Consul UI does for folders.
func (f *fakeConsul) folder(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index++
	f.kv[key] = entry{createIndex: f.index, modifyIndex: f.index}
	f.wake()
}

// remove deletes key.
func (f *fakeConsul) remove(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index++
	delete(f.kv, key)
	f.wake()
}

// setIndex overrides the current index, for example to make it go
// backwards as after a snapshot restore. Like a restore it abandons all
// blocking queries, which return the current index at once.
func (f *fakeConsul) setIndex(i uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index = i
	f.epoch++
	f.wake()
}

// failNext makes the next requests fail with the given status codes.
func (f *fakeConsul) failNext(codes ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = append(f.fail, codes...)
}

// stats returns the number of served queries and of currently blocked ones.
func (f *fakeConsul) stats() (queries, blocked int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries, f.blocked
}

// wake releases all blocking queries; f.mu must be held.
func (f *fakeConsul) wake() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeConsul) serve(w http.ResponseWriter, r *http.Request) {
	key, ok := strings.CutPrefix(r.URL.Path, "/v1/kv/")
	if !ok || r.Method != http.MethodGet {
		http.Error(w, "unsupported", http.StatusNotImplemented)
		return
	}
	q := r.URL.Query()
	_, recurse := q["recurse"]

	f.mu.Lock()
	f.queries++
	if len(f.fail) > 0 {
		code := f.fail[0]
		f.fail = f.fail[1:]
		f.mu.Unlock()
		http.Error(w, "injected failure", code)
		return
	}
	if wait, _ := strconv.ParseUint(q.Get("index"), 10, 64); wait > 0 && f.index <= wait {
		timeout := 5 * time.Minute
		if d, err := time.ParseDuration(q.Get("wait")); err == nil && d > 0 {
			timeout = d
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		f.blocked++
		epoch := f.epoch
		for expired := false; f.index <= wait && f.epoch == epoch && !expired && r.Context().Err() == nil; {
			changed := f.changed
			f.mu.Unlock()
			select {
			case <-changed:
			case <-timer.C:
				expired = true
			case <-r.Context().Done():
			}
			f.mu.Lock()
		}
		f.blocked--
	}
	var pairs []*api.KVPair
	for _, k := range slices.Sorted(maps.Keys(f.kv)) {
		if k == key || (recurse && strings.HasPrefix(k, key)) {
			e := f.kv[k]
			pairs = append(pairs, &api.KVPair{Key: k, Value: e.value, CreateIndex: e.createIndex, ModifyIndex: e.modifyIndex})
		}
	}
	index := f.index
	f.mu.Unlock()

	w.Header().Set("X-Consul-Index", strconv.FormatUint(index, 10))
	w.Header().Set("X-Consul-KnownLeader", "true")
	w.Header().Set("X-Consul-LastContact", "0")
	if len(pairs) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, pairs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
