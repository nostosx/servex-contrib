package grpcx_test

import (
	"context"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	testpb "google.golang.org/grpc/interop/grpc_testing"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/selector"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

// pinned is a Selector that picks the node with the pinned address when it
// is in the node set and the first node otherwise.
type pinned struct{ addr atomic.Pointer[string] }

func (p *pinned) pin(addr string) { p.addr.Store(&addr) }

func (p *pinned) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	if len(nodes) == 0 {
		return selector.Node{}, nil, selector.ErrNoAvailable
	}
	if a := p.addr.Load(); a != nil {
		for _, n := range nodes {
			if n.Addr == *a {
				return n, func(context.Context, error) {}, nil
			}
		}
	}
	return nodes[0], func(context.Context, error) {}, nil
}

// eventually polls cond until it holds or a deadline expires.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestClientEvictsConnectionsOfRemovedNodes checks that the connection to a
// node that left discovery is closed once its in-flight calls and streams
// finish, across repeated rollouts, and that Close releases all connections.
func TestClientEvictsConnectionsOfRemovedNodes(t *testing.T) {
	const addrA, addrB = "127.0.0.1:9110", "127.0.0.1:9111"
	n := newNetwork()
	startServer(t, n, addrA)
	_, svcB := startServer(t, n, addrB)
	reg := memory.New()
	insA := &discovery.Instance{ID: "a", Name: "svc", Endpoints: []string{"grpc://" + addrA}}
	insB := &discovery.Instance{ID: "b", Name: "svc", Endpoints: []string{"grpc://" + addrB}}
	for _, ins := range []*discovery.Instance{insA, insB} {
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	baseline := runtime.NumGoroutine()

	sel := &pinned{}
	c, err := grpcx.NewClient(t.Context(), "discovery:///svc",
		grpcx.WithDiscoverer(reg), grpcx.WithSelector(sel), grpcx.WithDialOptions(n.dialOption()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	client := testpb.NewTestServiceClient(c.Conn())

	call := func(addr string) string {
		t.Helper()
		sel.pin(addr)
		resp, err := client.UnaryCall(t.Context(), simple("x"))
		if err != nil {
			t.Fatalf("UnaryCall: %v", err)
		}
		return resp.GetServerId()
	}
	conns := func(want ...string) func() bool {
		return func() bool { return slices.Equal(c.ConnAddrs(), want) }
	}
	// deregisterB removes B and waits until the client offers the selector a
	// node set without it.
	deregisterB := func() {
		t.Helper()
		if err := reg.Deregister(t.Context(), insB); err != nil {
			t.Fatal(err)
		}
		eventually(t, "B to leave the node set", func() bool { return call(addrB) == addrA })
	}

	for round := range 3 {
		if err := reg.Register(t.Context(), insB); err != nil {
			t.Fatal(err)
		}
		eventually(t, "B to join the node set", func() bool { return call(addrB) == addrB })
		call(addrA)
		if got := c.ConnAddrs(); !slices.Equal(got, []string{addrA, addrB}) {
			t.Fatalf("round %d: conns = %v, want both nodes", round, got)
		}

		// Keep a server stream, a cancelable stream and a unary call in
		// flight on B.
		sel.pin(addrB)
		stream, err := client.StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{
			ResponseParameters: []*testpb.ResponseParameters{{}, {}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("Recv: %v", err)
		}
		cancelCtx, cancelStream := context.WithCancel(t.Context())
		abandoned, err := client.StreamingOutputCall(cancelCtx, &testpb.StreamingOutputCallRequest{
			ResponseParameters: []*testpb.ResponseParameters{{}, {}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := abandoned.Recv(); err != nil {
			t.Fatalf("Recv: %v", err)
		}
		upload, err := client.StreamingInputCall(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := upload.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("abc")}}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		blockCtx, cancelBlock := context.WithCancel(t.Context())
		var wg sync.WaitGroup
		wg.Go(func() { _, _ = client.UnaryCall(blockCtx, simple("block")) })
		<-svcB.blocked

		deregisterB()
		if got := c.ConnAddrs(); !slices.Equal(got, []string{addrA, addrB}) {
			t.Fatalf("round %d: conns with calls in flight = %v, want B kept", round, got)
		}

		// Finishing the streams and the unary call keeps B for the abandoned
		// stream.
		for {
			if _, err := stream.Recv(); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("Recv: %v", err)
			}
		}
		if resp, err := upload.CloseAndRecv(); err != nil || resp.GetAggregatedPayloadSize() != 3 {
			t.Fatalf("CloseAndRecv = %v, %v", resp, err)
		}
		cancelBlock()
		wg.Wait()
		if got := c.ConnAddrs(); !slices.Equal(got, []string{addrA, addrB}) {
			t.Fatalf("round %d: conns with a stream open = %v, want B kept", round, got)
		}

		// Canceling the abandoned stream releases the last use of B.
		cancelStream()
		eventually(t, "B's connection to be evicted", conns(addrA))
		eventually(t, "B's network connection to close", func() bool { return n.open.Load() == 1 })
		call(addrA)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	eventually(t, "all network connections to close", func() bool { return n.open.Load() == 0 })
	eventually(t, "client goroutines to exit", func() bool { return runtime.NumGoroutine() <= baseline })
}

// TestClientKeepsConnectionsOfRemainingNodes checks that node set changes
// leave the connections of nodes that stay in the set alone.
func TestClientKeepsConnectionsOfRemainingNodes(t *testing.T) {
	const addrA, addrB = "127.0.0.1:9112", "127.0.0.1:9113"
	n := newNetwork()
	startServer(t, n, addrA)
	startServer(t, n, addrB)
	reg := memory.New()
	insA := &discovery.Instance{ID: "a", Name: "svc", Endpoints: []string{"grpc://" + addrA}}
	if err := reg.Register(t.Context(), insA); err != nil {
		t.Fatal(err)
	}
	sel := &pinned{}
	c := newClient(t, n, "discovery:///svc", grpcx.WithDiscoverer(reg), grpcx.WithSelector(sel))
	client := testpb.NewTestServiceClient(c.Conn())
	if _, err := client.UnaryCall(t.Context(), simple("x")); err != nil {
		t.Fatal(err)
	}
	insB := &discovery.Instance{ID: "b", Name: "svc", Endpoints: []string{"grpc://" + addrB}}
	if err := reg.Register(t.Context(), insB); err != nil {
		t.Fatal(err)
	}
	sel.pin(addrB)
	eventually(t, "B to join the node set", func() bool {
		resp, err := client.UnaryCall(t.Context(), simple("x"))
		return err == nil && resp.GetServerId() == addrB
	})
	if got := c.ConnAddrs(); !slices.Equal(got, []string{addrA, addrB}) {
		t.Errorf("conns = %v, want both nodes", got)
	}
	if n.dials.Load() != 2 {
		t.Errorf("dials = %d, want 2", n.dials.Load())
	}
}

// TestClientEvictsIdleConnectionWithoutCalls checks that the idle
// connection of a node that left discovery is closed as soon as the node set
// changes, without waiting for another call.
func TestClientEvictsIdleConnectionWithoutCalls(t *testing.T) {
	const addrA, addrB = "127.0.0.1:9114", "127.0.0.1:9115"
	n := newNetwork()
	startServer(t, n, addrA)
	startServer(t, n, addrB)
	reg := memory.New()
	insA := &discovery.Instance{ID: "a", Name: "svc", Endpoints: []string{"grpc://" + addrA}}
	insB := &discovery.Instance{ID: "b", Name: "svc", Endpoints: []string{"grpc://" + addrB}}
	for _, ins := range []*discovery.Instance{insA, insB} {
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	sel := &pinned{}
	c := newClient(t, n, "discovery:///svc", grpcx.WithDiscoverer(reg), grpcx.WithSelector(sel))
	client := testpb.NewTestServiceClient(c.Conn())
	for _, addr := range []string{addrA, addrB} {
		sel.pin(addr)
		if _, err := client.UnaryCall(t.Context(), simple("x")); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.ConnAddrs(); !slices.Equal(got, []string{addrA, addrB}) {
		t.Fatalf("conns = %v, want both nodes", got)
	}
	if err := reg.Deregister(t.Context(), insB); err != nil {
		t.Fatal(err)
	}
	eventually(t, "B's idle connection to be evicted", func() bool { return slices.Equal(c.ConnAddrs(), []string{addrA}) })
	eventually(t, "B's network connection to close", func() bool { return n.open.Load() == 1 })
}
