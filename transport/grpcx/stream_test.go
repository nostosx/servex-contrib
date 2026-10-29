package grpcx_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

type streamPrincipal struct{}

// controlledStream keeps an RPC open after its first response so tests can
// distinguish stream creation, half-close and complete-RPC observation.
type controlledStream struct {
	testpb.UnimplementedTestServiceServer
	started chan context.Context
	finish  chan struct{}
}

func (s *controlledStream) StreamingOutputCall(req *testpb.StreamingOutputCallRequest, stream grpc.ServerStreamingServer[testpb.StreamingOutputCallResponse]) error {
	s.started <- stream.Context()
	if string(req.GetPayload().GetBody()) == "panic" {
		panic("handler panic")
	}
	if err := stream.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
		return err
	}
	select {
	case <-s.finish:
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (*controlledStream) FullDuplexCall(stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&testpb.StreamingOutputCallResponse{Payload: request.GetPayload()}); err != nil {
			return err
		}
	}
}

func startControlledStream(t *testing.T, n *network, opts ...grpcx.ServerOption) *controlledStream {
	t.Helper()
	svc := &controlledStream{started: make(chan context.Context, 1), finish: make(chan struct{})}
	srv := grpcx.NewServer(append([]grpcx.ServerOption{grpcx.WithListener(n.listen("127.0.0.1:9400"))}, opts...)...)
	testpb.RegisterTestServiceServer(srv, svc)
	if err := srv.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return svc
}

func awaitStreamResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("stream middleware did not settle")
		var zero T
		return zero
	}
}

func observeStream(t *testing.T, kind metadata.Kind, ended chan<- error, enter func(context.Context) context.Context) endpoint.Middleware {
	t.Helper()
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, ok := metadata.FromContext(ctx)
			if !ok || info.Kind != kind || info.Protocol != "grpc" || info.Operation != testpb.TestService_StreamingOutputCall_FullMethodName {
				t.Errorf("stream metadata = %+v", info)
			}
			stream, ok := req.(*grpcx.StreamInfo)
			if !ok || stream.ClientStreams || !stream.ServerStreams {
				t.Errorf("stream request = %+v", req)
			}
			if enter != nil {
				ctx = enter(ctx)
			}
			response, err := next(ctx, req)
			if err == nil && (stream.SentMessages() != 1 || stream.ReceivedMessages() != 1) {
				t.Errorf("completed stream counters = sent %d, received %d", stream.SentMessages(), stream.ReceivedMessages())
			}
			ended <- err
			return response, err
		}
	}
}

func TestStreamingMiddlewareFullLifecycle(t *testing.T) {
	n := newNetwork()
	serverEnd, clientEnd := make(chan error, 1), make(chan error, 1)
	auth := observeStream(t, metadata.KindServer, serverEnd, func(ctx context.Context) context.Context {
		info, _ := metadata.FromContext(ctx)
		if info.Header.Get("authorization") != "Bearer valid" {
			t.Errorf("authorization = %q", info.Header.Get("authorization"))
		}
		return context.WithValue(ctx, streamPrincipal{}, "user-1")
	})
	svc := startControlledStream(t, n, grpcx.WithStreamMiddleware(auth))
	inject := observeStream(t, metadata.KindClient, clientEnd, func(ctx context.Context) context.Context {
		info, _ := metadata.FromContext(ctx)
		info.Header.Set("authorization", "Bearer valid")
		return context.WithValue(ctx, streamPrincipal{}, "client-context")
	})
	c := newClient(t, n, "grpc://127.0.0.1:9400", grpcx.WithClientStreamMiddleware(inject))
	stream, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Fatal(err)
	}
	serverCtx := awaitStreamResult(t, svc.started)
	if serverCtx.Value(streamPrincipal{}) != "user-1" || stream.Context().Value(streamPrincipal{}) != "client-context" {
		t.Error("stream middleware context was not propagated")
	}
	// The generated client already called CloseSend. That only closes the
	// send direction; the call-level chain must remain on the stack.
	for _, ended := range []chan error{serverEnd, clientEnd} {
		select {
		case err := <-ended:
			t.Fatalf("middleware ended before the stream: %v", err)
		default:
		}
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	close(svc.finish)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final receive = %v", err)
	}
	if err := awaitStreamResult(t, serverEnd); err != nil {
		t.Errorf("server middleware = %v", err)
	}
	if err := awaitStreamResult(t, clientEnd); err != nil {
		t.Errorf("client middleware = %v", err)
	}
}

func TestStreamingMiddlewareCancellationAndClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller cancellation", true: "client close"}[closeClient], func(t *testing.T) {
			n := newNetwork()
			svc := startControlledStream(t, n)
			ended := make(chan error, 1)
			c := newClient(t, n, "grpc://127.0.0.1:9400", grpcx.WithClientStreamMiddleware(observeStream(t, metadata.KindClient, ended, nil)))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if _, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{}); err != nil {
				t.Fatal(err)
			}
			awaitStreamResult(t, svc.started)
			if closeClient {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			// No Recv call is needed to unwind middleware after cancellation.
			if err := awaitStreamResult(t, ended); errorx.CodeOf(err) != errorx.Canceled {
				t.Errorf("middleware result = %v, want Canceled", err)
			}
		})
	}
}

func TestClientStreamingMiddlewareBeforeCreation(t *testing.T) {
	for _, tt := range []struct {
		name string
		body endpoint.Endpoint
		code errorx.Code
	}{
		{"auth rejection", func(context.Context, any) (any, error) {
			return nil, errorx.New(errorx.Unauthenticated, "TOKEN", "missing token")
		}, errorx.Unauthenticated},
		{"panic", func(context.Context, any) (any, error) { panic("middleware panic") }, errorx.Internal},
		{"missing next", func(context.Context, any) (any, error) { return nil, nil }, errorx.FailedPrecondition},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mw := func(endpoint.Endpoint) endpoint.Endpoint { return tt.body }
			n := newNetwork()
			c := newClient(t, n, "grpc://127.0.0.1:9400", grpcx.WithClientStreamMiddleware(mw))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{})
			if errorx.CodeOf(err) != tt.code {
				t.Fatalf("NewStream = %v, want %v", err, tt.code)
			}
			if n.dials.Load() != 0 {
				t.Error("rejected stream dialed the server")
			}
		})
	}
	t.Run("no nodes", func(t *testing.T) {
		n := newNetwork()
		ended := make(chan error, 1)
		c := newClient(t, n, "discovery:///empty", grpcx.WithDiscoverer(memory.New()), grpcx.WithClientStreamMiddleware(observeStream(t, metadata.KindClient, ended, nil)))
		_, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{})
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatal(err)
		}
		if err := awaitStreamResult(t, ended); errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatal(err)
		}
	})
}

func TestStreamingMiddlewareClientStreaming(t *testing.T) {
	n := newNetwork()
	startServer(t, n, "127.0.0.1:9401")
	ended := make(chan error, 1)
	observe := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info := req.(*grpcx.StreamInfo)
			if !info.ClientStreams || info.ServerStreams {
				t.Errorf("stream shape = %+v", info)
			}
			response, err := next(ctx, req)
			if err == nil && (info.SentMessages() != 1 || info.ReceivedMessages() != 1) {
				t.Errorf("completed client-stream counters = sent %d, received %d", info.SentMessages(), info.ReceivedMessages())
			}
			ended <- err
			return response, err
		}
	}
	c := newClient(t, n, "grpc://127.0.0.1:9401", grpcx.WithClientStreamMiddleware(observe))
	stream, err := testpb.NewTestServiceClient(c.Conn()).StreamingInputCall(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("data")}}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.CloseAndRecv()
	if err != nil || response.GetAggregatedPayloadSize() != 4 {
		t.Fatalf("CloseAndRecv = %v, %v", response, err)
	}
	if err := awaitStreamResult(t, ended); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingMiddlewareBidirectional(t *testing.T) {
	n := newNetwork()
	counts := make(chan [2]int64, 2)
	observe := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			info := request.(*grpcx.StreamInfo)
			if !info.ClientStreams || !info.ServerStreams {
				t.Errorf("bidirectional stream shape = %+v", info)
			}
			response, err := next(ctx, request)
			counts <- [2]int64{info.SentMessages(), info.ReceivedMessages()}
			return response, err
		}
	}
	startControlledStream(t, n, grpcx.WithStreamMiddleware(observe))
	c := newClient(t, n, "grpc://127.0.0.1:9400", grpcx.WithClientStreamMiddleware(observe))
	stream, err := testpb.NewTestServiceClient(c.Conn()).FullDuplexCall(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := stream.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("echo")}}); err != nil {
			t.Fatal(err)
		}
		response, err := stream.Recv()
		if err != nil || string(response.GetPayload().GetBody()) != "echo" {
			t.Fatalf("echo = %v, %v", response, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	for range 2 {
		if result := awaitStreamResult(t, counts); result != [2]int64{3, 3} {
			t.Errorf("stream messages = %v, want [3 3]", result)
		}
	}
}

func TestStreamingMiddlewareErrorAndReplay(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "remote error", true: "replay rejected"}[replay], func(t *testing.T) {
			n := newNetwork()
			startServer(t, n, "127.0.0.1:9402")
			var calls atomic.Int32
			ended := make(chan error, 1)
			mw := func(next endpoint.Endpoint) endpoint.Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					calls.Add(1)
					response, err := next(ctx, req)
					if replay {
						response, err = next(ctx, req)
					}
					ended <- err
					return response, err
				}
			}
			c := newClient(t, n, "grpc://127.0.0.1:9402", grpcx.WithClientStreamMiddleware(mw))
			stream, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{
				ResponseStatus: &testpb.EchoStatus{Code: int32(errorx.ResourceExhausted), Message: "quota"},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Recv()
			want := errorx.ResourceExhausted
			if replay {
				want = errorx.FailedPrecondition
			}
			if errorx.CodeOf(err) != want || errorx.CodeOf(awaitStreamResult(t, ended)) != want || calls.Load() != 1 {
				t.Errorf("terminal error = %v, middleware executions = %d", err, calls.Load())
			}
		})
	}
}

func TestServerStreamingRecoveryAndHealthBypass(t *testing.T) {
	t.Run("recovery", func(t *testing.T) {
		n := newNetwork()
		recovery := func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (response any, err error) {
				defer func() {
					if recover() != nil {
						err = errorx.New(errorx.Internal, "PANIC", "recovered")
					}
				}()
				return next(ctx, req)
			}
		}
		startControlledStream(t, n, grpcx.WithStreamMiddleware(recovery))
		c := newClient(t, n, "grpc://127.0.0.1:9400")
		stream, err := testpb.NewTestServiceClient(c.Conn()).StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("panic")}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); errorx.CodeOf(err) != errorx.Internal {
			t.Fatal(err)
		}
	})
	t.Run("health watch", func(t *testing.T) {
		n := newNetwork()
		deny := func(endpoint.Endpoint) endpoint.Endpoint {
			return func(context.Context, any) (any, error) {
				return nil, errorx.New(errorx.Unauthenticated, "TOKEN", "denied")
			}
		}
		startServer(t, n, "127.0.0.1:9403", grpcx.WithStreamMiddleware(deny), grpcx.WithHealth(health.CheckerFunc(func(context.Context) error { return nil })))
		c := newClient(t, n, "grpc://127.0.0.1:9403")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stream, err := grpc_health_v1.NewHealthClient(c.Conn()).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		response, err := stream.Recv()
		if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("health = %v, %v", response, err)
		}
	})
}
