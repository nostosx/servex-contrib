package transport_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/selector"
)

// nodeRecorder collects the node sets passed to a node observer.
type nodeRecorder struct {
	mu    sync.Mutex
	calls [][]selector.Node
}

func (r *nodeRecorder) observe(nodes []selector.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, nodes)
}

func (r *nodeRecorder) snapshot() [][]selector.Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// weightSelector records the nodes offered to it and picks the first.
type weightSelector struct {
	mu      sync.Mutex
	offered []selector.Node
}

func (s *weightSelector) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	s.mu.Lock()
	s.offered = slices.Clone(nodes)
	s.mu.Unlock()
	if len(nodes) == 0 {
		return selector.Node{}, nil, selector.ErrNoAvailable
	}
	return nodes[0], func(context.Context, error) {}, nil
}

func TestInstanceWeightReachesNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		go func() {
			w.results <- watchResult{instances: []*discovery.Instance{
				{ID: "a", Weight: 5, Metadata: map[string]string{"zone": "a"}, Endpoints: []string{"http://10.0.0.1:80"}},
				{ID: "b", Endpoints: []string{"http://10.0.0.2:80"}},
			}}
		}()
		sel := &weightSelector{}
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"), sxclient.WithSelector(sel))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.Endpoint("op", addrInvoker())(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		sel.mu.Lock()
		defer sel.mu.Unlock()
		if len(sel.offered) != 2 || sel.offered[0].Weight != 5 || sel.offered[1].Weight != 0 ||
			sel.offered[0].Metadata["zone"] != "a" {
			t.Errorf("offered nodes = %+v, want weights 5 and 0", sel.offered)
		}
	})
}

func TestNodeObserverDirectTarget(t *testing.T) {
	rec := &nodeRecorder{}
	c, err := newClient(t.Context(), "http://10.0.0.1:80", sxclient.WithNodeObserver(rec.observe))
	if err != nil {
		t.Fatal(err)
	}
	// The first call happens before NewClient returns.
	if calls := rec.snapshot(); len(calls) != 1 || len(calls[0]) != 1 || calls[0][0].Addr != "10.0.0.1:80" {
		t.Fatalf("calls before NewClient returned = %+v", calls)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.snapshot()); n != 1 {
		t.Errorf("observer called %d times, want 1", n)
	}
}

func TestNodeObserverDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		rec := &nodeRecorder{}
		go func() { w.results <- watchResult{} }() // an empty first snapshot
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"),
			sxclient.WithNodeObserver(rec.observe))
		if err != nil {
			t.Fatal(err)
		}
		if calls := rec.snapshot(); len(calls) != 1 || len(calls[0]) != 0 {
			t.Fatalf("calls before NewClient returned = %+v, want one empty set", calls)
		}

		send := func(instances ...*discovery.Instance) {
			t.Helper()
			w.results <- watchResult{instances: instances}
			synctest.Wait()
		}
		a := &discovery.Instance{ID: "a", Weight: 1, Metadata: map[string]string{"v": "1"}, Endpoints: []string{"http://10.0.0.1:80"}}
		send(a)
		// The same nodes, from another instance or a non-matching endpoint, are not a change.
		send(&discovery.Instance{ID: "a2", Weight: 1, Metadata: map[string]string{"v": "1"},
			Endpoints: []string{"http://10.0.0.1:80", "grpc://10.0.0.1:90"}})
		// Weight and metadata changes are changes.
		send(&discovery.Instance{ID: "a", Weight: 2, Metadata: map[string]string{"v": "1"}, Endpoints: []string{"http://10.0.0.1:80"}})
		send(&discovery.Instance{ID: "a", Weight: 2, Metadata: map[string]string{"v": "2"}, Endpoints: []string{"http://10.0.0.1:80"}})
		send() // back to empty

		calls := rec.snapshot()
		want := [][]selector.Node{
			{},
			{{Addr: "10.0.0.1:80", Weight: 1, Metadata: map[string]string{"v": "1"}}},
			{{Addr: "10.0.0.1:80", Weight: 2, Metadata: map[string]string{"v": "1"}}},
			{{Addr: "10.0.0.1:80", Weight: 2, Metadata: map[string]string{"v": "2"}}},
			{},
		}
		if !slices.EqualFunc(calls, want, func(a, b []selector.Node) bool {
			return slices.EqualFunc(a, b, func(x, y selector.Node) bool {
				return x.Addr == y.Addr && x.Weight == y.Weight && maps.Equal(x.Metadata, y.Metadata)
			})
		}) {
			t.Fatalf("observer calls = %+v, want %+v", calls, want)
		}

		// The observer owns its copy: mutating it changes neither the client
		// nor the instance it came from.
		calls[1][0].Metadata["v"] = "mutated"
		if a.Metadata["v"] != "1" {
			t.Error("observer copy shares metadata with the instance")
		}

		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if n := len(rec.snapshot()); n != len(want) {
			t.Errorf("observer called %d times after Close", n-len(want))
		}
	})
}

// The observer is called from the watch goroutine; Close waits for a call in
// progress, so no call happens after Close returns.
func TestNodeObserverNotCalledAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		entered := make(chan struct{})
		release := make(chan struct{})
		var mu sync.Mutex
		calls, closed := 0, false
		go func() { w.results <- watchResult{} }()
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"),
			sxclient.WithNodeObserver(func([]selector.Node) {
				mu.Lock()
				calls++
				if closed {
					t.Error("observer called after Close returned")
				}
				n := calls
				mu.Unlock()
				if n == 2 {
					close(entered)
					<-release
				}
			}))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			w.results <- watchResult{instances: []*discovery.Instance{instance("a", "http://10.0.0.1:80")}}
		}()
		<-entered
		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			_ = c.Close()
			mu.Lock()
			closed = true
			mu.Unlock()
		}()
		synctest.Wait()
		select {
		case <-closeDone:
			t.Fatal("Close returned while the observer was running")
		default:
		}
		close(release)
		<-closeDone
	})
}
