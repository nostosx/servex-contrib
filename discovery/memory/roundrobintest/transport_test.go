package roundrobin_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex-contrib/selector/roundrobin"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// retryN is a minimal retry middleware calling next up to n times while it
// fails with a retryable error.
func retryN(n int) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (resp any, err error) {
			for range n {
				if resp, err = next(ctx, req); !errorx.IsRetryable(err) {
					return resp, err
				}
			}
			return resp, err
		}
	}
}

// newClient registers addrs as instances of "svc" and returns a client
// resolving them through discovery with the round-robin selector.
func newClient(t *testing.T, addrs ...string) *sxclient.Client {
	t.Helper()
	reg := memory.New()
	for i, addr := range addrs {
		ins := &discovery.Instance{ID: fmt.Sprint("i", i), Name: "svc", Endpoints: []string{"test://" + addr}}
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	c, err := sxclient.New(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg),
		sxclient.WithScheme("test"),
		sxclient.WithSelector(roundrobin.New()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// recorder is an Invoker recording the addresses it is called with and
// failing on the addresses in down.
type recorder struct {
	down map[string]bool

	mu    sync.Mutex
	addrs []string
}

func (r *recorder) Invoke(_ context.Context, addr string, req any) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = append(r.addrs, addr)
	if r.down[addr] {
		return nil, errorx.Newf(errorx.Unavailable, "DOWN", "%s is down", addr)
	}
	return addr, nil
}

func (r *recorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.addrs)
}

func TestTransportClientRotates(t *testing.T) {
	c := newClient(t, "10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80")
	rec := &recorder{}
	ep := c.Endpoint("/svc/Get", rec)
	for range 9 {
		if _, err := ep(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for _, addr := range rec.calls() {
		counts[addr]++
	}
	if len(counts) != 3 {
		t.Fatalf("calls reached %v, want all 3 nodes", counts)
	}
	for addr, n := range counts {
		if n != 3 {
			t.Errorf("node %s got %d of 9 calls, want 3", addr, n)
		}
	}
}

// TestTransportClientInstanceWeights checks that weights registered in
// discovery reach the selector: sxclient.Client copies Instance.Weight to
// Node.Weight.
func TestTransportClientInstanceWeights(t *testing.T) {
	reg := memory.New()
	for i, w := range []int{3, 1} {
		ins := &discovery.Instance{ID: fmt.Sprint("i", i), Name: "svc", Weight: w, Endpoints: []string{fmt.Sprintf("test://10.0.0.%d:80", i+1)}}
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	c, err := sxclient.New(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg),
		sxclient.WithScheme("test"),
		sxclient.WithSelector(roundrobin.New()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	rec := &recorder{}
	ep := c.Endpoint("/svc/Get", rec)
	for range 8 {
		if _, err := ep(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for _, addr := range rec.calls() {
		counts[addr]++
	}
	if counts["10.0.0.1:80"] != 6 || counts["10.0.0.2:80"] != 2 {
		t.Fatalf("calls = %v, want 6 and 2 for weights 3 and 1", counts)
	}
}

// TestTransportClientRetryChangesNode checks that a retry wrapped around the
// client endpoint selects a different node for each attempt.
func TestTransportClientRetryChangesNode(t *testing.T) {
	c := newClient(t, "10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80")
	rec := &recorder{down: map[string]bool{"10.0.0.1:80": true, "10.0.0.2:80": true}}
	ep := c.Endpoint("/svc/Get", rec, retryN(3))
	resp, err := ep(t.Context(), nil)
	if err != nil || resp != "10.0.0.3:80" {
		t.Fatalf("ep = %v, %v; want success on the healthy node", resp, err)
	}
	calls := rec.calls()
	if len(calls) > 3 {
		t.Fatalf("attempts = %v, want at most 3", calls)
	}
	seen := map[string]bool{}
	for _, addr := range calls {
		if seen[addr] {
			t.Fatalf("attempts = %v: a node was retried before trying the others", calls)
		}
		seen[addr] = true
	}
}
