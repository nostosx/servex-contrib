# transport/connectx

Unreleased. Requires matching servex sources containing `transport/rpc`.
The core requirement is a release preparation baseline, not a claim that these
new APIs are already available from the published core. Uses Connect-Go v1.21.1.

Implements core `rpc.UnaryHandler` and adapts core typed request/response and
stream interfaces to Connect-Go. One handler negotiates Connect, gRPC and
gRPC-Web, with Protobuf and JSON encoding. No conversion proxy is required.

## Server

```go
package main

import (
	"context"
	"log"
	"net/http"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nostosx/servex/transport/rpc"
	"github.com/nostosx/servex-contrib/transport/connectx"
)

func main() {
	const procedure = "/example.v1.Echo/Call"
	handler := connectx.UnaryFunc[wrapperspb.StringValue, wrapperspb.StringValue](
		func(ctx context.Context, req *rpc.Request[wrapperspb.StringValue]) (*rpc.Response[wrapperspb.StringValue], error) {
			return &rpc.Response[wrapperspb.StringValue]{Msg: wrapperspb.String(req.Msg.Value)}, nil
		})
	mux := http.NewServeMux()
	mux.Handle(procedure, connectx.NewUnaryHandler(procedure, handler))
	server := &http.Server{Addr: "127.0.0.1:8080", Handler: mux, Protocols: connectx.Protocols()}
	log.Fatal(server.ListenAndServe())
}
```

For application lifecycle integration, construct contrib `httpx.NewServer` with
`httpx.WithProtocols(connectx.Protocols())`, then `HandleHTTP(procedure, handler)`.
HTTP/1 remains available for normal routes. HTTP/2 and unencrypted HTTP/2 (h2c)
enable native gRPC and bidirectional streams on the same listener. Configure
TLS, authentication and timeouts at the composition root. Browser cross-origin
requests additionally need the application's CORS middleware.

There is no custom IDL generator. Use normal Protobuf messages and full
`/package.Service/Method` paths. Existing generated Connect handlers also work:
install the exported interceptors in their normal connect.HandlerOption list.
Core contracts contain no Connect or Protobuf SDK types.

## Calls and streams

- `NewUnaryHandler` accepts core `rpc.UnaryHandler`; `UnaryFunc` adapts a function.
  Requests expose headers; responses expose headers and trailers.
- `NewServerStreamHandler` accepts a core request and `rpc.ServerStream`.
- `NewClientStreamHandler` receives `rpc.ClientStream`; Receive returns a
  message or `io.EOF`. Return a typed core response when input completes.
- `NewBidiStreamHandler` receives `rpc.BidiStream`, allowing one sender and one
  receiver concurrently. Do not use a stream after its handler returns.
- `NewClient[Req, Res](httpClient, fullProcedureURL, options...)` implements the
  core UnaryHandler. Select `connect.WithGRPC()` or `connect.WithGRPCWeb()` to
  override the default Connect protocol. `SDK()` exposes standard streaming
  client methods and SDK extension points. The HTTP client remains caller-owned.

Native gRPC and bidirectional RPCs require HTTP/2. Go clients are tested with
all three protocols and all four RPC shapes over HTTP/2. Browser Fetch and
gRPC-Web clients have platform-specific streaming restrictions; these Go tests
do not establish browser client-streaming or bidirectional support. Connect
server streaming is also tested over HTTP/1.1. Cancellation stops server work.

## Middleware and errors

Adapter-created handlers automatically populate servex metadata.Info with call
direction, negotiated protocol, full procedure and headers. Install additional
middleware with `connect.WithInterceptors(connectx.UnaryMiddleware(...))` or
`connectx.StreamMiddleware(...)`. Unary middleware receives `connect.AnyRequest`
and returns `connect.AnyResponse`; preserve the concrete request/response types.
Client unary middleware sees decoded errorx errors and may add outbound headers
through metadata.Info.Header. NewClient installs the metadata/error bridge.

StreamMiddleware explicitly wraps the whole server stream lifetime. Its request
is a `connect.StreamingHandlerConn` and its successful reply is nil. Choose
middleware appropriate to stream ownership; retrying a partially consumed or
sent stream is not safe. This module does not implement client stream lifetime
middleware or automatic replay. Native Connect interceptors remain available.

ToError and FromError preserve canonical error codes, ErrorInfo reason and
metadata, and the reserved `servex.retryable` decision shared with grpcx.
Received errors are marked remote so propagation does not multiply retries
across service hops. Caller cancellation stays local and preserves errors.Is.
Unrecognized application errors expose a generic message. Successful unary
handlers must return a non-nil response and message.

The tests use real loopback HTTP/1.1 and HTTP/2 connections. They cover protocol
negotiation, all RPC shapes, headers/trailers, middleware lifetimes, errors and
cancellation; no external backend or credentials are required.
