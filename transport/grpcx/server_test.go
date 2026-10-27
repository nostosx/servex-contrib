package grpcx_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

// rawConn dials addr on n with a plain grpc.ClientConn.
func rawConn(t *testing.T, n *network, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), n.dialOption())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

func TestServerUnaryMiddlewareAndInfo(t *testing.T) {
	n := newNetwork()
	var order []string
	var seen metadata.Info
	mark := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				order = append(order, name)
				if name == "outer" {
					seen, _ = metadata.FromContext(ctx)
					// Typed rejects a request of another type with a TypeError.
					check := endpoint.Typed(func(context.Context, *testpb.SimpleRequest) (any, error) { return nil, nil })
					if _, err := check(ctx, req); err != nil {
						t.Errorf("req = %T, want *SimpleRequest: %v", req, err)
					}
				}
				resp, err := next(ctx, req)
				if r, ok := resp.(*testpb.SimpleResponse); ok && name == "outer" {
					r.Username += "+mw"
				}
				return resp, err
			}
		}
	}
	srv, svc := startServer(t, n, "127.0.0.1:9001", grpcx.Middleware(mark("outer"), mark("inner")))
	if got := srv.URL().String(); got != "grpc://127.0.0.1:9001" {
		t.Errorf("URL = %s", got)
	}
	if srv.Name() != "grpc" {
		t.Errorf("Name = %q", srv.Name())
	}
	if got := grpcx.NewServer(grpcx.WithName("grpc:admin")).Name(); got != "grpc:admin" {
		t.Errorf("Name with WithName = %q, want grpc:admin", got)
	}
	if got := grpcx.NewServer(grpcx.WithName("")).Name(); got != "grpc" {
		t.Errorf("Name with an empty WithName = %q, want grpc", got)
	}
	if _, ok := srv.GetServiceInfo()["grpc.testing.TestService"]; !ok {
		t.Error("TestService not in GetServiceInfo")
	}

	client := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9001"))
	ctx := grpcmd.AppendToOutgoingContext(t.Context(), "X-User", "neo", "x-multi", "a", "x-multi", "b")
	resp, err := client.UnaryCall(ctx, simple("hi"))
	if err != nil {
		t.Fatalf("UnaryCall: %v", err)
	}
	if resp.GetUsername() != "neo+mw" || string(resp.GetPayload().GetBody()) != "hi" {
		t.Errorf("resp = %v", resp)
	}
	if !slices.Equal(order, []string{"outer", "inner"}) {
		t.Errorf("order = %v", order)
	}
	want := "/grpc.testing.TestService/UnaryCall"
	if seen.Kind != metadata.KindServer || seen.Protocol != "grpc" || seen.Operation != want {
		t.Errorf("info = %+v", seen)
	}
	if !slices.Equal(seen.Header.Values("x-multi"), []string{"a", "b"}) || seen.Header.Get("x-user") != "neo" {
		t.Errorf("header = %v", seen.Header)
	}
	if got := svc.lastInfo(); got.Operation != want {
		t.Errorf("handler info = %+v", got)
	}
}

func TestServerMiddlewareShortCircuit(t *testing.T) {
	n := newNetwork()
	deny := func(endpoint.Endpoint) endpoint.Endpoint {
		return func(context.Context, any) (any, error) {
			return nil, errorx.New(errorx.Unauthenticated, "NO_TOKEN", "missing token")
		}
	}
	_, svc := startServer(t, n, "127.0.0.1:9002", grpcx.Middleware(deny))
	_, err := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9002")).UnaryCall(t.Context(), simple("hi"))
	st := status.Convert(err)
	if st.Code() != codes.Unauthenticated || st.Message() != "missing token" {
		t.Fatalf("status = %v", st)
	}
	if len(svc.infos) != 0 {
		t.Error("handler called despite middleware rejection")
	}
}

func TestServerMiddlewareDroppingContext(t *testing.T) {
	n := newNetwork()
	drop := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(_ context.Context, req any) (any, error) { return next(context.Background(), req) }
	}
	startServer(t, n, "127.0.0.1:9003", grpcx.Middleware(drop))
	_, err := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9003")).UnaryCall(t.Context(), simple("hi"))
	if status.Code(err) != codes.Unknown {
		t.Fatalf("err = %v, want Unknown", err)
	}
}

func TestServerErrorMapping(t *testing.T) {
	n := newNetwork()
	startServer(t, n, "127.0.0.1:9004")
	client := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9004"))

	_, err := client.UnaryCall(t.Context(), simple("notfound"))
	st := status.Convert(err)
	if st.Code() != codes.NotFound || st.Message() != "user not found" {
		t.Fatalf("status = %v", st)
	}
	var info *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if i, ok := d.(*errdetails.ErrorInfo); ok {
			info = i
		}
	}
	if info == nil || info.GetReason() != "USER_NOT_FOUND" || info.GetMetadata()["id"] != "404" {
		t.Errorf("ErrorInfo = %v", info)
	}

	if _, err := client.UnaryCall(t.Context(), simple("status")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("status passthrough = %v", err)
	}
	if _, err := client.UnaryCall(t.Context(), simple("plain")); status.Code(err) != codes.Unknown {
		t.Errorf("plain error = %v", err)
	}
}

func TestServerStreamInfoWithoutMiddleware(t *testing.T) {
	n := newNetwork()
	var calls atomic.Int32
	count := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			calls.Add(1)
			return next(ctx, req)
		}
	}
	_, svc := startServer(t, n, "127.0.0.1:9005", grpcx.Middleware(count))
	client := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9005"))
	ctx := grpcmd.AppendToOutgoingContext(t.Context(), "x-user", "trinity")
	stream, err := client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{
		ResponseParameters: []*testpb.ResponseParameters{{}, {}},
		ResponseStatus:     &testpb.EchoStatus{Code: int32(codes.Aborted), Message: "stop"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if got := string(msg.GetPayload().GetBody()); got != "127.0.0.1:9005 trinity" {
			t.Errorf("body = %q", got)
		}
	}
	_, err = stream.Recv()
	st := status.Convert(err)
	if st.Code() != codes.Aborted || st.Message() != "stop" || len(st.Details()) != 1 {
		t.Errorf("final status = %v", st)
	}
	info := svc.lastInfo()
	if info.Kind != metadata.KindServer || info.Protocol != "grpc" || info.Operation != "/grpc.testing.TestService/StreamingOutputCall" {
		t.Errorf("info = %+v", info)
	}
	if calls.Load() != 0 {
		t.Errorf("middleware ran %d times for a stream", calls.Load())
	}
}

func TestServerHealth(t *testing.T) {
	n := newNetwork()
	var healthy atomic.Bool
	healthy.Store(true)
	checker := health.CheckerFunc(func(context.Context) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("down")
	})
	var mwCalls atomic.Int32
	mw := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			mwCalls.Add(1)
			return next(ctx, req)
		}
	}
	srv, _ := startServer(t, n, "127.0.0.1:9006",
		grpcx.Health(checker), grpcx.WithHealthInterval(5*time.Millisecond), grpcx.Middleware(mw))
	hc := grpc_health_v1.NewHealthClient(rawConn(t, n, "127.0.0.1:9006"))

	check := func(service string) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
		resp, err := hc.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: service})
		return resp.GetStatus(), err
	}
	for _, svc := range []string{"", "grpc.testing.TestService", "grpc.health.v1.Health"} {
		if st, err := check(svc); err != nil || st != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("Check(%q) = %v, %v", svc, st, err)
		}
	}
	if _, err := check("no.such.Service"); status.Code(err) != codes.NotFound {
		t.Errorf("unknown service err = %v", err)
	}
	if mwCalls.Load() != 0 {
		t.Errorf("health probes ran middleware %d times", mwCalls.Load())
	}

	list, err := hc.List(t.Context(), &grpc_health_v1.HealthListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetStatuses()) != 3 || list.GetStatuses()[""].GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("List = %v", list.GetStatuses())
	}

	unknown, err := hc.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: "no.such.Service"})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := unknown.Recv(); err != nil || resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Errorf("Watch(unknown) = %v, %v", resp, err)
	}

	watch, err := hc.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	recv := func() grpc_health_v1.HealthCheckResponse_ServingStatus {
		t.Helper()
		resp, err := watch.Recv()
		if err != nil {
			t.Fatalf("Watch.Recv: %v", err)
		}
		return resp.GetStatus()
	}
	if st := recv(); st != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("first watch status = %v", st)
	}
	healthy.Store(false)
	if st := recv(); st != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("watch status after failure = %v", st)
	}
	if st, _ := check(""); st != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf("Check after failure = %v", st)
	}
	healthy.Store(true)
	if st := recv(); st != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("watch status after recovery = %v", st)
	}

	// Stop ends open Watch streams instead of waiting for them.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := recv(); st != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf("watch status on stop = %v", st)
	}
	if _, err := watch.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("watch end = %v, want Unavailable", err)
	}
}

func TestServerLifecycle(t *testing.T) {
	srv := grpcx.NewServer(grpcx.Address("127.0.0.1:0"))
	if srv.URL() != nil {
		t.Error("URL before Start != nil")
	}
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	u := srv.URL()
	if u.Scheme != "grpc" || u.Port() == "0" || u.Hostname() != "127.0.0.1" {
		t.Errorf("URL = %v", u)
	}
	u.Host = "mutated"
	if srv.URL().Host == "mutated" {
		t.Error("URL returned a shared value")
	}
	if err := srv.Start(t.Context()); err == nil {
		t.Error("second Start succeeded")
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err, ok := <-srv.Err(); ok {
		t.Errorf("Err() delivered %v after a clean stop", err)
	}
	if err := srv.Start(t.Context()); err == nil {
		t.Error("Start after Stop succeeded")
	}

	// Stopping a server that never started is a no-op.
	if err := grpcx.NewServer().Stop(t.Context()); err != nil {
		t.Errorf("Stop unstarted: %v", err)
	}

	// Listen failures are reported by Start.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	busy := grpcx.NewServer(grpcx.Address(ln.Addr().String()))
	if err := busy.Start(t.Context()); err == nil {
		_ = busy.Stop(t.Context()) // best-effort cleanup of the unexpected server
		t.Error("Start on a busy address succeeded")
	}
}

func TestServerStopTimeoutForcesClose(t *testing.T) {
	n := newNetwork()
	srv, svc := startServer(t, n, "127.0.0.1:9007")
	client := testpb.NewTestServiceClient(rawConn(t, n, "127.0.0.1:9007"))
	errc := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(context.Background(), simple("block"))
		errc <- err
	}()
	select {
	case <-svc.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("call never reached the handler")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := srv.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if err := <-errc; status.Code(err) != codes.Unavailable && status.Code(err) != codes.Canceled {
		t.Errorf("blocked call = %v", err)
	}
}

// failingListener fails Accept with a permanent error.
type failingListener struct{ net.Listener }

func (failingListener) Accept() (net.Conn, error) { return nil, errors.New("accept failed") }

func TestServerServeErrorReported(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpcx.NewServer(grpcx.WithListener(failingListener{ln}))
	if err := srv.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := srv.Stop(t.Context()); err != nil {
			t.Errorf("Stop() = %v", err)
		}
	}()
	select {
	case err := <-srv.Err():
		if err == nil {
			t.Fatal("Err() delivered nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve error not reported")
	}
}
