package p2c_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex-contrib/selector/p2c"
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

// clock is a manually advanced clock shared by the selector and the fake
// invoker, which advances it by the latency of each node.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeNodes is an Invoker simulating nodes with fixed latencies, failing on
// the nodes in down, and recording the order of attempts.
type fakeNodes struct {
	clock   *clock
	latency map[string]time.Duration
	down    map[string]bool

	mu    sync.Mutex
	addrs []string
}

func (f *fakeNodes) Invoke(_ context.Context, addr string, _ any) (any, error) {
	f.clock.Advance(f.latency[addr])
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrs = append(f.addrs, addr)
	if f.down[addr] {
		return nil, errorx.Newf(errorx.Unavailable, "DOWN", "%s is down", addr)
	}
	return addr, nil
}

func (f *fakeNodes) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.addrs)
}

func TestTransportClient(t *testing.T) {
	addrs := []string{"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"}
	reg := memory.New()
	for i, addr := range addrs {
		ins := &discovery.Instance{ID: fmt.Sprint("i", i), Name: "svc", Endpoints: []string{"test://" + addr}}
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	clk := &clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c, err := sxclient.New(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg),
		sxclient.WithScheme("test"),
		sxclient.WithSelector(p2c.New(p2c.WithClock(clk.Now))),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	nodes := &fakeNodes{
		clock: clk,
		latency: map[string]time.Duration{
			addrs[0]: 5 * time.Millisecond,
			addrs[1]: 50 * time.Millisecond,
			addrs[2]: time.Millisecond, // fast but failing
		},
		down: map[string]bool{addrs[2]: true},
	}
	ep := c.Endpoint("/svc/Get", nodes, retryN(3))

	const calls = 2000
	for i := range calls {
		if _, err := ep(t.Context(), nil); err != nil {
			t.Fatalf("call %d: %v (attempts %v)", i, err, nodes.calls())
		}
		clk.Advance(time.Millisecond)
	}

	attempts := nodes.calls()
	// Retries never repeat the failing node within one call: the failure
	// reported through Done makes the next attempt pick another node.
	for i := 1; i < len(attempts); i++ {
		if attempts[i-1] == addrs[2] && attempts[i] == addrs[2] {
			t.Fatalf("attempts %d and %d both hit the failing node", i-1, i)
		}
	}
	counts := map[string]int{}
	for _, a := range attempts {
		counts[a]++
	}
	if counts[addrs[2]] > calls/10 {
		t.Errorf("failing node got %d attempts for %d calls, want it avoided: %v", counts[addrs[2]], calls, counts)
	}
	if counts[addrs[0]] <= counts[addrs[1]] {
		t.Errorf("fast node got %d attempts, slow node %d: want the fast one preferred", counts[addrs[0]], counts[addrs[1]])
	}
}
