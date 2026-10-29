package connectx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nostosx/servex-contrib/transport/connectx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/transport/rpc"
)

type message = wrapperspb.StringValue

const prefix = "/test.v1.Echo/"

func TestProtocolsAndStreaming(t *testing.T) {
	t.Parallel()
	var unaryCalls, streamCalls atomic.Int32
	unaryMW := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, ok := metadata.FromContext(ctx)
			if !ok || info.Kind != metadata.KindServer || info.Protocol == "" || info.Operation != prefix+"Unary" || info.Header.Get("x-test") != "request" {
				t.Errorf("metadata=%+v", info)
			}
			unaryCalls.Add(1)
			return next(ctx, req)
		}
	}
	streamMW := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, ok := metadata.FromContext(ctx)
			if !ok || info.Kind != metadata.KindServer || info.Protocol == "" {
				t.Errorf("stream metadata=%+v", info)
			}
			streamCalls.Add(1)
			return next(ctx, req)
		}
	}
	mux := http.NewServeMux()
	mux.Handle(prefix+"Unary", connectx.NewUnaryHandler(prefix+"Unary", connectx.UnaryFunc[message, message](func(_ context.Context, req *rpc.Request[message]) (*rpc.Response[message], error) {
		if req.Msg.Value == "error" {
			return nil, errorx.New(errorx.Unavailable, "TEST_DOWN", "try later").WithMetadata(map[string]string{"backend": "test"}).WithRetryable(false)
		}
		return &rpc.Response[message]{Msg: wrapperspb.String(req.Msg.Value), Header: http.Header{"X-Reply": {"header"}}, Trailer: http.Header{"X-End": {"trailer"}}}, nil
	}), connect.WithInterceptors(connectx.UnaryMiddleware(unaryMW))))
	streamOpts := connect.WithInterceptors(connectx.StreamMiddleware(streamMW))
	mux.Handle(prefix+"Server", connectx.NewServerStreamHandler(prefix+"Server", func(_ context.Context, req *rpc.Request[message], stream rpc.ServerStream[message]) error {
		stream.ResponseHeader().Set("X-Reply", "stream")
		stream.ResponseTrailer().Set("X-End", "done")
		for _, value := range []string{req.Msg.Value, "second"} {
			if err := stream.Send(wrapperspb.String(value)); err != nil {
				return err
			}
		}
		return nil
	}, streamOpts))
	mux.Handle(prefix+"Client", connectx.NewClientStreamHandler(prefix+"Client", func(_ context.Context, stream rpc.ClientStream[message]) (*rpc.Response[message], error) {
		var text string
		for {
			msg, err := stream.Receive()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			text += msg.Value
		}
		return &rpc.Response[message]{Msg: wrapperspb.String(text)}, nil
	}, streamOpts))
	mux.Handle(prefix+"Bidi", connectx.NewBidiStreamHandler(prefix+"Bidi", func(_ context.Context, stream rpc.BidiStream[message, message]) error {
		for {
			msg, err := stream.Receive()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}, streamOpts))
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	for _, tc := range []struct {
		name string
		opts []connect.ClientOption
	}{{"connect", nil}, {"grpc", []connect.ClientOption{connect.WithGRPC()}}, {"grpc-web", []connect.ClientOption{connect.WithGRPCWeb()}}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client := connectx.NewClient[message, message](server.Client(), server.URL+prefix+"Unary", tc.opts...)
			req := &rpc.Request[message]{Msg: wrapperspb.String("hello"), Header: http.Header{"X-Test": {"request"}}}
			res, err := client.Call(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if res.Msg.Value != "hello" || res.Header.Get("X-Reply") != "header" || res.Trailer.Get("X-End") != "trailer" {
				t.Fatalf("response=%+v", res)
			}
			req.Msg = wrapperspb.String("error")
			_, err = client.Call(ctx, req)
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok || e.Code != errorx.Unavailable || e.Reason != "TEST_DOWN" || e.Metadata["backend"] != "test" || e.Retryable() || !e.Remote() {
				t.Fatalf("wire error=%v", err)
			}
			ss := connect.NewClient[message, message](server.Client(), server.URL+prefix+"Server", tc.opts...)
			stream, err := ss.CallServerStream(ctx, connect.NewRequest(wrapperspb.String("first")))
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			for _, want := range []string{"first", "second"} {
				if !stream.Receive() || stream.Msg().Value != want {
					t.Fatalf("server stream: %v", stream.Err())
				}
			}
			if stream.Receive() || stream.Err() != nil || stream.ResponseHeader().Get("X-Reply") != "stream" || stream.ResponseTrailer().Get("X-End") != "done" {
				t.Fatalf("stream completion=%v", stream.Err())
			}
			cs := connect.NewClient[message, message](server.Client(), server.URL+prefix+"Client", tc.opts...).CallClientStream(ctx)
			for _, text := range []string{"a", "b"} {
				if err := cs.Send(wrapperspb.String(text)); err != nil {
					t.Fatal(err)
				}
			}
			combined, err := cs.CloseAndReceive()
			if err != nil || combined.Msg.Value != "ab" {
				t.Fatalf("client stream: response=%v error=%v", combined, err)
			}
			bidi := connect.NewClient[message, message](server.Client(), server.URL+prefix+"Bidi", tc.opts...).CallBidiStream(ctx)
			defer func() { _ = bidi.CloseResponse() }()
			if err := bidi.Send(wrapperspb.String("echo")); err != nil {
				t.Fatal(err)
			}
			msg, err := bidi.Receive()
			if err != nil || msg.Value != "echo" {
				t.Fatalf("bidi: message=%v error=%v", msg, err)
			}
			if err := bidi.CloseRequest(); err != nil {
				t.Fatal(err)
			}
			if _, err := bidi.Receive(); !errors.Is(err, io.EOF) {
				t.Fatalf("bidi end=%v", err)
			}
		})
	}
	if unaryCalls.Load() != 6 || streamCalls.Load() != 9 {
		t.Fatalf("middleware calls: unary=%d stream=%d", unaryCalls.Load(), streamCalls.Load())
	}
}

func TestHTTP1AndCancellation(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	finished := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle(prefix+"Wait", connectx.NewServerStreamHandler(prefix+"Wait", func(ctx context.Context, _ *rpc.Request[message], stream rpc.ServerStream[message]) error {
		defer close(finished)
		close(started)
		if err := stream.Send(wrapperspb.String("ready")); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}))
	server := httptest.NewServer(mux)
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := connect.NewClient[message, message](server.Client(), server.URL+prefix+"Wait")
	stream, err := client.CallServerStream(ctx, connect.NewRequest(wrapperspb.String("wait")))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Receive() || stream.Msg().Value != "ready" {
		t.Fatal(stream.Err())
	}
	<-started
	cancel()
	if stream.Receive() || connect.CodeOf(stream.Err()) != connect.CodeCanceled {
		t.Fatalf("cancel error=%v", stream.Err())
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("server stream did not cancel")
	}
}

func TestErrorIdentityAndRetryPropagation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	converted := connectx.FromError(ctx, connect.NewError(connect.CodeCanceled, context.Canceled))
	e := errorx.From(converted)
	if !errors.Is(converted, context.Canceled) || e.Remote() {
		t.Fatalf("cancel=%v", converted)
	}
	original := errorx.New(errorx.Unavailable, "REMOTE", "down").WithRemote()
	decoded := errorx.From(connectx.FromError(t.Context(), connectx.ToError(original)))
	if decoded.Retryable() || decoded.Reason != "REMOTE" {
		t.Fatalf("propagated error=%v", decoded)
	}
	if err := connectx.ToError(errors.New("private implementation detail")); err.Error() == "private implementation detail" || connect.CodeOf(err) != connect.CodeUnknown {
		t.Fatalf("unknown=%v", err)
	}
}

func TestUnaryMiddlewareErrorBoundary(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	serverMiddleware := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			res, err := next(ctx, req)
			if err != nil && errorx.CodeOf(err) != errorx.Unavailable {
				t.Errorf("server middleware lost application error: %v", err)
			}
			return res, err
		}
	}
	handler := connectx.NewUnaryHandler(prefix+"Retry", connectx.UnaryFunc[message, message](func(_ context.Context, req *rpc.Request[message]) (*rpc.Response[message], error) {
		if req.Header.Get("X-Auth") != "injected" {
			t.Error("outbound middleware header missing")
		}
		if calls.Add(1) == 1 {
			return nil, errorx.New(errorx.Unavailable, "RETRY_ONCE", "retry once")
		}
		return &rpc.Response[message]{Msg: wrapperspb.String("done")}, nil
	}), connect.WithInterceptors(connectx.UnaryMiddleware(serverMiddleware)))
	server := httptest.NewServer(handler)
	defer server.Close()
	clientMiddleware := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, ok := metadata.FromContext(ctx)
			if !ok || info.Kind != metadata.KindClient {
				t.Fatalf("client metadata=%+v", info)
			}
			info.Header.Set("x-auth", "injected")
			res, err := next(ctx, req)
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok || e.Reason != "RETRY_ONCE" || !e.Remote() || !e.Retryable() {
				t.Errorf("middleware did not receive decoded error: %v", err)
				return res, err
			}
			return next(ctx, req) // Explicit unary policy; the adapter itself never retries.
		}
	}
	client := connectx.NewClient[message, message](server.Client(), server.URL+prefix+"Retry", connect.WithInterceptors(connectx.UnaryMiddleware(clientMiddleware)))
	res, err := client.Call(t.Context(), &rpc.Request[message]{Msg: wrapperspb.String("test")})
	if err != nil || res.Msg.Value != "done" || calls.Load() != 2 {
		t.Fatalf("response=%v calls=%d error=%v", res, calls.Load(), err)
	}
}

func TestStreamFailureAndEmptyResponse(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.Handle(prefix+"Fail", connectx.NewServerStreamHandler(prefix+"Fail", func(_ context.Context, _ *rpc.Request[message], stream rpc.ServerStream[message]) error {
		calls.Add(1)
		if err := stream.Send(wrapperspb.String("partial")); err != nil {
			return err
		}
		return errorx.New(errorx.Unavailable, "PARTIAL", "stream ended")
	}))
	mux.Handle(prefix+"Empty", connectx.NewUnaryHandler(prefix+"Empty", connectx.UnaryFunc[message, message](func(context.Context, *rpc.Request[message]) (*rpc.Response[message], error) { return nil, nil })))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := connect.NewClient[message, message](server.Client(), server.URL+prefix+"Fail")
	stream, err := client.CallServerStream(t.Context(), connect.NewRequest(wrapperspb.String("test")))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Receive() || stream.Msg().Value != "partial" {
		t.Fatalf("first receive=%v", stream.Err())
	}
	if stream.Receive() {
		t.Fatal("unexpected second message")
	}
	decoded := errorx.From(connectx.FromError(t.Context(), stream.Err()))
	if decoded.Code != errorx.Unavailable || decoded.Reason != "PARTIAL" || calls.Load() != 1 {
		t.Fatalf("stream calls=%d error=%v", calls.Load(), decoded)
	}
	unary := connectx.NewClient[message, message](server.Client(), server.URL+prefix+"Empty")
	if _, err := unary.Call(t.Context(), &rpc.Request[message]{Msg: wrapperspb.String("test")}); errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("empty response error=%v", err)
	}
	if _, err := unary.Call(t.Context(), nil); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("empty request error=%v", err)
	}
}
