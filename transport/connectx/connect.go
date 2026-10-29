// Package connectx adapts the core typed RPC contracts to Connect-Go. Its
// handlers speak Connect, gRPC and gRPC-Web and mount through HandleHTTP.
package connectx

import (
	"context"
	"io"
	"maps"
	"net/http"

	"connectrpc.com/connect"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport/rpc"
)

// UnaryFunc adapts an application function to rpc.UnaryHandler.
type UnaryFunc[Req, Res any] func(context.Context, *rpc.Request[Req]) (*rpc.Response[Res], error)

func (f UnaryFunc[Req, Res]) Call(ctx context.Context, req *rpc.Request[Req]) (*rpc.Response[Res], error) {
	return f(ctx, req)
}

// NewUnaryHandler creates a handler for a full procedure path, for example
// /example.v1.Greeter/Greet. SDK options remain local to this adapter.
func NewUnaryHandler[Req, Res any](procedure string, handler rpc.UnaryHandler[Req, Res], opts ...connect.HandlerOption) *connect.Handler {
	return connect.NewUnaryHandler(procedure, func(ctx context.Context, req *connect.Request[Req]) (*connect.Response[Res], error) {
		res, err := handler.Call(ctx, &rpc.Request[Req]{Msg: req.Msg, Header: req.Header()})
		if err != nil {
			return nil, err
		}
		return sdkResponse(res)
	}, handlerOptions(opts)...)
}

// NewServerStreamHandler adapts a server-streaming application handler.
func NewServerStreamHandler[Req, Res any](procedure string, handler func(context.Context, *rpc.Request[Req], rpc.ServerStream[Res]) error, opts ...connect.HandlerOption) *connect.Handler {
	return connect.NewServerStreamHandler(procedure, func(ctx context.Context, req *connect.Request[Req], stream *connect.ServerStream[Res]) error {
		return handler(ctx, &rpc.Request[Req]{Msg: req.Msg, Header: req.Header()}, stream)
	}, handlerOptions(opts)...)
}

// NewClientStreamHandler adapts a client-streaming application handler.
func NewClientStreamHandler[Req, Res any](procedure string, handler func(context.Context, rpc.ClientStream[Req]) (*rpc.Response[Res], error), opts ...connect.HandlerOption) *connect.Handler {
	return connect.NewClientStreamHandler(procedure, func(ctx context.Context, stream *connect.ClientStream[Req]) (*connect.Response[Res], error) {
		res, err := handler(ctx, &clientStream[Req]{stream})
		if err != nil {
			return nil, err
		}
		return sdkResponse(res)
	}, handlerOptions(opts)...)
}

// NewBidiStreamHandler adapts a bidirectional application handler. HTTP/2 is
// required for bidirectional streams; browsers do not expose full duplex.
func NewBidiStreamHandler[Req, Res any](procedure string, handler func(context.Context, rpc.BidiStream[Req, Res]) error, opts ...connect.HandlerOption) *connect.Handler {
	return connect.NewBidiStreamHandler(procedure, func(ctx context.Context, stream *connect.BidiStream[Req, Res]) error {
		return handler(ctx, stream)
	}, handlerOptions(opts)...)
}

func handlerOptions(opts []connect.HandlerOption) []connect.HandlerOption {
	return append([]connect.HandlerOption{connect.WithInterceptors(UnaryMiddleware(), StreamMiddleware())}, opts...)
}

type clientStream[T any] struct{ *connect.ClientStream[T] }

var (
	_ rpc.UnaryHandler[any, any] = UnaryFunc[any, any](nil)
	_ rpc.UnaryHandler[any, any] = (*Client[any, any])(nil)
	_ rpc.ClientStream[any]      = (*clientStream[any])(nil)
	_ rpc.ServerStream[any]      = (*connect.ServerStream[any])(nil)
	_ rpc.BidiStream[any, any]   = (*connect.BidiStream[any, any])(nil)
)

func (s *clientStream[T]) Receive() (*T, error) {
	if s.ClientStream.Receive() {
		return s.Msg(), nil
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func sdkResponse[T any](res *rpc.Response[T]) (*connect.Response[T], error) {
	if res == nil || res.Msg == nil {
		return nil, errorx.New(errorx.Internal, "RPC_RESPONSE", "handler returned an empty response")
	}
	out := connect.NewResponse(res.Msg)
	maps.Copy(out.Header(), res.Header.Clone())
	maps.Copy(out.Trailer(), res.Trailer.Clone())
	return out, nil
}

// Client implements rpc.UnaryHandler. SDK exposes generated-compatible
// streaming calls; it never takes ownership of the supplied HTTP client.
type Client[Req, Res any] struct{ sdk *connect.Client[Req, Res] }

// NewClient creates a typed client for the full procedure URL. Connect is the
// default; select connect.WithGRPC or connect.WithGRPCWeb for other protocols.
func NewClient[Req, Res any](client connect.HTTPClient, url string, opts ...connect.ClientOption) *Client[Req, Res] {
	options := append([]connect.ClientOption{connect.WithInterceptors(UnaryMiddleware())}, opts...)
	return &Client[Req, Res]{sdk: connect.NewClient[Req, Res](client, url, options...)}
}

// SDK returns the underlying client for streaming calls and SDK extensions.
func (c *Client[Req, Res]) SDK() *connect.Client[Req, Res] { return c.sdk }

func (c *Client[Req, Res]) Call(ctx context.Context, req *rpc.Request[Req]) (*rpc.Response[Res], error) {
	if req == nil || req.Msg == nil {
		return nil, errorx.New(errorx.InvalidArgument, "RPC_REQUEST", "request message is required")
	}
	out := connect.NewRequest(req.Msg)
	maps.Copy(out.Header(), req.Header.Clone())
	res, err := c.sdk.CallUnary(ctx, out)
	if err != nil {
		return nil, FromError(ctx, err)
	}
	return &rpc.Response[Res]{Msg: res.Msg, Header: res.Header().Clone(), Trailer: res.Trailer().Clone()}, nil
}

// Protocols enables HTTP/1 and HTTP/2 (including h2c). Pass it to
// httpx.WithProtocols or http.Server.Protocols so native gRPC and bidirectional
// RPCs share the HTTP listener. Configure TLS and access policy separately.
func Protocols() *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	return p
}
