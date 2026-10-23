package grpcx_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/discovery/memory"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

// newClient creates a grpcx.Client dialing through n.
func newClient(t *testing.T, n *network, target string, opts ...grpcx.ClientOption) *grpcx.Client {
	t.Helper()
	c, err := grpcx.NewClient(t.Context(), target, append([]grpcx.ClientOption{grpcx.WithDialOptions(n.dialOption())}, opts...)...)
	if err != nil {
		t.Fatalf("NewClient(%q): %v", target, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestNewClientInvalidTargets(t *testing.T) {
	for _, target := range []string{"", "http://127.0.0.1:1", "127.0.0.1:1", "grpc://", "grpc://[::1", "discovery:///svc"} {
		c, err := grpcx.NewClient(t.Context(), target)
		if err == nil {
			c.Close()
			t.Errorf("NewClient(%q) succeeded, want error", target)
		}
	}
}

func TestClientUnaryMiddlewareAndHeader(t *testing.T) {
	n := newNetwork()
	_, svc := startServer(t, n, "127.0.0.1:9101")
	var order []string
	var seen metadata.Info
	mark := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				order = append(order, name)
				info, _ := metadata.FromContext(ctx)
				if name == "first" {
					seen = info
					if r, ok := req.(*testpb.SimpleRequest); !ok || string(r.GetPayload().GetBody()) != "hello" {
						t.Errorf("req = %v", req)
					}
				}
				info.Header.Add("x-chain", name)
				return next(ctx, req)
			}
		}
	}
	c := newClient(t, n, "grpc://127.0.0.1:9101", grpcx.WithClientMiddleware(mark("first"), mark("second")))
	client := testpb.NewTestServiceClient(c.Conn())

	ctx := grpcmd.AppendToOutgoingContext(t.Context(), "x-user", "neo")
	var header grpcmd.MD
	resp, err := client.UnaryCall(ctx, simple("hello"), grpc.Header(&header))
	if err != nil {
		t.Fatalf("UnaryCall: %v", err)
	}
	if resp.GetServerId() != "127.0.0.1:9101" || resp.GetUsername() != "neo" || string(resp.GetPayload().GetBody()) != "hello" {
		t.Errorf("resp = %v", resp)
	}
	if header == nil {
		t.Error("call options were not forwarded")
	}
	if !slices.Equal(order, []string{"first", "second"}) {
		t.Errorf("order = %v", order)
	}
	if seen.Kind != metadata.KindClient || seen.Protocol != "grpc" || seen.Operation != "/grpc.testing.TestService/UnaryCall" {
		t.Errorf("client info = %+v", seen)
	}
	got := svc.lastInfo().Header
	if !slices.Equal(got.Values("x-chain"), []string{"first", "second"}) || got.Get("x-user") != "neo" {
		t.Errorf("server header = %v", got)
	}

	// Each call starts with a fresh header; the endpoint is cached per method.
	order = nil
	if _, err := client.UnaryCall(t.Context(), simple("hello")); err != nil {
		t.Fatal(err)
	}
	if got := svc.lastInfo().Header.Values("x-chain"); !slices.Equal(got, []string{"first", "second"}) {
		t.Errorf("second call header = %v", got)
	}
}

func TestClientErrorsAreErrorx(t *testing.T) {
	n := newNetwork()
	startServer(t, n, "127.0.0.1:9102")
	c := newClient(t, n, "grpc://127.0.0.1:9102")
	client := testpb.NewTestServiceClient(c.Conn())

	_, err := client.UnaryCall(t.Context(), simple("notfound"))
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok {
		t.Fatalf("err = %T %v, want *errorx.Error", err, err)
	}
	if !errors.Is(err, errUserNotFound) || e.Code != errorx.NotFound || e.Message != "user not found" ||
		e.Metadata["id"] != "404" || e.Retryable() {
		t.Errorf("err = %+v", e)
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("status.Code = %v", status.Code(err))
	}

	_, err = client.UnaryCall(t.Context(), simple("unavailable"))
	if e, _ := errors.AsType[*errorx.Error](err); e == nil || e.Code != errorx.Unavailable || e.Reason != "BUSY" || !e.Retryable() {
		t.Errorf("unavailable err = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(ctx, simple("block"))
		errc <- err
	}()
	cancel()
	if err := <-errc; errorx.CodeOf(err) != errorx.Canceled || !errors.Is(err, context.Canceled) {
		t.Errorf("canceled err = %v", err)
	}
}

func TestClientConnectionFailureIsRetryable(t *testing.T) {
	n := newNetwork()
	// Nothing listens on this address: the dialer fails.
	c := newClient(t, n, "grpc://127.0.0.1:9199")
	_, err := testpb.NewTestServiceClient(c.Conn()).UnaryCall(t.Context(), simple("x"))
	if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
		t.Fatalf("err = %v, want retryable Unavailable", err)
	}
}

func TestClientMiddlewareShortCircuit(t *testing.T) {
	n := newNetwork()
	deny := func(endpoint.Endpoint) endpoint.Endpoint {
		return func(context.Context, any) (any, error) {
			return nil, errorx.New(errorx.PermissionDenied, "LOCAL", "denied locally")
		}
	}
	c := newClient(t, n, "grpc://127.0.0.1:9103", grpcx.WithClientMiddleware(deny))
	_, err := testpb.NewTestServiceClient(c.Conn()).UnaryCall(t.Context(), simple("x"))
	if e, _ := errors.AsType[*errorx.Error](err); e == nil || e.Reason != "LOCAL" {
		t.Fatalf("err = %v", err)
	}
	if n.dials.Load() != 0 || c.ConnCount() != 0 {
		t.Error("a connection was opened despite the short circuit")
	}

	drop := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(_ context.Context, req any) (any, error) { return next(context.Background(), req) }
	}
	c2 := newClient(t, n, "grpc://127.0.0.1:9103", grpcx.WithClientMiddleware(drop))
	_, err = testpb.NewTestServiceClient(c2.Conn()).UnaryCall(t.Context(), simple("x"))
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("dropped context err = %v, want Internal", err)
	}
}

func TestClientNodeSelectionPoolingAndRetry(t *testing.T) {
	n := newNetwork()
	startServer(t, n, "127.0.0.1:9104")
	startServer(t, n, "127.0.0.1:9105")
	reg := memory.New()
	for i, addr := range []string{"127.0.0.1:9104", "127.0.0.1:9105"} {
		ins := &discovery.Instance{ID: string(rune('a' + i)), Name: "svc", Endpoints: []string{"http://" + addr, "grpc://" + addr}}
		if err := reg.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	sel := &roundRobin{}
	// retryOnce calls next again when the first attempt fails.
	retryOnce := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			resp, err := next(ctx, req)
			if errorx.IsRetryable(err) {
				return next(ctx, req)
			}
			return resp, err
		}
	}
	c := newClient(t, n, "discovery:///svc", grpcx.WithDiscoverer(reg), grpcx.WithSelector(sel), grpcx.WithClientMiddleware(retryOnce))
	client := testpb.NewTestServiceClient(c.Conn())

	var ids []string
	for range 4 {
		resp, err := client.UnaryCall(t.Context(), simple("x"))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, resp.GetServerId())
	}
	if ids[0] == ids[1] || ids[0] != ids[2] || ids[1] != ids[3] {
		t.Errorf("server ids = %v, want alternating nodes", ids)
	}
	if c.ConnCount() != 2 || n.dials.Load() != 2 {
		t.Errorf("conns = %d, dials = %d; want one per node", c.ConnCount(), n.dials.Load())
	}

	// A retryable failure is retried on the next node by the middleware,
	// because node selection is the innermost step.
	_, err := client.UnaryCall(t.Context(), simple("unavailable"))
	if errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("err = %v", err)
	}
	outcomes := sel.outcomes()
	if len(outcomes) != 6 || outcomes[4] == nil || outcomes[5] == nil {
		t.Errorf("selector outcomes = %v", outcomes)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if c.ConnCount() != 0 {
		t.Errorf("conns after Close = %d", c.ConnCount())
	}
	_, err = client.UnaryCall(t.Context(), simple("x"))
	if e, _ := errors.AsType[*errorx.Error](err); e == nil || e.Code != errorx.FailedPrecondition || e.Reason != "CLIENT_CLOSED" {
		t.Errorf("call after Close = %v", err)
	}
}

func TestClientNoNodes(t *testing.T) {
	n := newNetwork()
	c := newClient(t, n, "discovery:///none", grpcx.WithDiscoverer(memory.New()))
	_, err := testpb.NewTestServiceClient(c.Conn()).UnaryCall(t.Context(), simple("x"))
	if !errors.Is(err, selector.ErrNoAvailable) {
		t.Errorf("unary err = %v", err)
	}
	_, err = testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{})
	if !errors.Is(err, selector.ErrNoAvailable) {
		t.Errorf("stream err = %v", err)
	}
}

func TestClientStream(t *testing.T) {
	n := newNetwork()
	startServer(t, n, "127.0.0.1:9106")
	startServer(t, n, "127.0.0.1:9107")
	reg := memory.New()
	for i, addr := range []string{"127.0.0.1:9106", "127.0.0.1:9107"} {
		if err := reg.Register(t.Context(), &discovery.Instance{ID: string(rune('a' + i)), Name: "svc", Endpoints: []string{"grpc://" + addr}}); err != nil {
			t.Fatal(err)
		}
	}
	var mwCalls int
	mw := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			mwCalls++
			return next(ctx, req)
		}
	}
	sel := &roundRobin{}
	c := newClient(t, n, "discovery:///svc", grpcx.WithDiscoverer(reg), grpcx.WithSelector(sel), grpcx.WithClientMiddleware(mw))
	client := testpb.NewTestServiceClient(c.Conn())

	var bodies []string
	for range 2 {
		ctx := grpcmd.AppendToOutgoingContext(t.Context(), "x-user", "morpheus")
		stream, err := client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{
			ResponseParameters: []*testpb.ResponseParameters{{}},
		})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		bodies = append(bodies, string(msg.GetPayload().GetBody()))
		if _, err := stream.Recv(); err != io.EOF {
			t.Errorf("end of stream = %v, want io.EOF", err)
		}
		if _, err := stream.Header(); err != nil {
			t.Errorf("Header: %v", err)
		}
	}
	if !slices.Equal(bodies, []string{"127.0.0.1:9106 morpheus", "127.0.0.1:9107 morpheus"}) {
		t.Errorf("bodies = %v", bodies)
	}
	if mwCalls != 0 {
		t.Errorf("client middleware ran %d times for streams", mwCalls)
	}

	// Stream errors are converted to errorx.
	stream, err := client.StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{
		ResponseStatus: &testpb.EchoStatus{Code: int32(codes.ResourceExhausted), Message: "quota"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	e, _ := errors.AsType[*errorx.Error](err)
	if e == nil || e.Code != errorx.ResourceExhausted || e.Reason != "STREAM_FAILED" || e.Message != "quota" || !e.Retryable() {
		t.Errorf("stream err = %v", err)
	}

	// Bidirectional streams work on the selected node as well.
	bidi, err := c.Conn().NewStream(t.Context(), &testpb.TestService_ServiceDesc.Streams[2], testpb.TestService_FullDuplexCall_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	if err := bidi.CloseSend(); err != nil {
		t.Errorf("CloseSend: %v", err)
	}
	var reply testpb.StreamingOutputCallResponse
	if err := bidi.RecvMsg(&reply); errorx.CodeOf(err) != errorx.Unimplemented {
		t.Errorf("FullDuplexCall err = %v, want Unimplemented", err)
	}
}
