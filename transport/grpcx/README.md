# transport/grpcx

Runnable server and client programs: [servex-examples](https://github.com/nostosx/servex-examples).

`github.com/nostosx/servex-contrib/transport/grpcx` implements the servex
transport contracts on [gRPC-Go](https://github.com/grpc/grpc-go), mirroring
the core `transport/httpx`.

- `Server` is a `transport.Server` (an `app.Hook` with a `grpc://` URL) and a
  `grpc.ServiceRegistrar`, so generated code registers directly:
  `pb.RegisterGreeterServer(srv, impl)`. Unary calls run the servex server
  middleware with `metadata.Info{Kind: server, Protocol: "grpc", Operation: full
  method, Header: incoming metadata}`. Streaming calls use a separate,
  explicitly configured stream middleware chain with the same metadata.
- `Client` exposes `Conn()`, a `grpc.ClientConnInterface` for generated clients
  (`pb.NewGreeterClient(c.Conn())`). Every unary call runs the client
  middleware, selects a node (direct `grpc://host:port` or
  `discovery:///<name>` target) and is made on that node's connection.
  Connections are kept per node and closed once the node leaves the set: the
  client follows node set changes through `client.WithNodeObserver`, so an
  idle connection closes as soon as discovery reports the change, and one in
  use after its last call or stream.
- Errors map between `errorx` and gRPC statuses with the same numeric code;
  `Reason` and `Metadata` travel in a `google.rpc.ErrorInfo` detail and come
  back as an `*errorx.Error` on the client. Retryability follows the code; a
  different decision travels under the reserved ErrorInfo metadata key
  `servex.retryable` (`RetryableMetadataKey`, value `true` or `false`).
  Application metadata under that key is dropped, and the client removes the
  key from `Metadata`.
- Client errors are marked remote (`errorx.WithRemote`): this client's retry
  middleware may retry them, but a server returning such an error, directly
  or wrapped, sends `PropagatedRetryable`, so its caller does not retry it
  again. Set `errorx.From(err).WithRetryable(true)` in the handler to let the
  caller retry. Statuses gRPC reports for connection failures are remote too;
  `Canceled` and `DeadlineExceeded` caused by the caller's own context are not.

## Streaming middleware

`WithStreamMiddleware` (server) and `WithClientStreamMiddleware` (client)
accept servex `endpoint.Middleware`. Each chain runs once around the complete
streaming RPC, including bidirectional and client-streaming calls. The request
is a `*grpcx.StreamInfo`, with `ClientStreams` / `ServerStreams` flags and
concurrent-safe `SentMessages()` / `ReceivedMessages()` counters. Treat the
flags as read-only. Counters count successful message operations on this side
of the connection and exclude EOF and failed operations; message contents are
never passed through the call-level middleware.

The following assembly fragments reuse the existing middleware module. Import
the corresponding `servex-contrib/middleware` packages in your application:

```go
srv := grpcx.NewServer(grpcx.WithStreamMiddleware(
    recovery.New(),
    tracing.New(tracer, propagator),
    logging.New(logger),
    metrics.New(meter),
    auth.New(verify),
))

client, err := grpcx.NewClient(ctx, target, grpcx.WithClientStreamMiddleware(
    tracing.New(tracer, propagator),
    logging.New(logger),
    metrics.New(meter),
    auth.Client(token),
))
```

Unary middleware remains separate. Authentication runs when the stream starts;
logging, request metrics and spans cover its full lifetime. To record message
counts, custom middleware can read the `StreamInfo` counters after `next`
returns. Per-message validation belongs in the handler or native gRPC stream
interceptors. Health Watch bypasses the server stream chain, like unary probes.

A stream must not be replayed. Do not add unary retry, request validation or
per-attempt timeout middleware to this chain. A second call to `next` is rejected
with non-retryable `FailedPrecondition` (`STREAM_REPLAYED`). Use the caller's
context to bound the whole RPC. Middleware that rejects a stream must return
an error; success without calling `next` is `STREAM_NOT_STARTED` on the client.

Client middleware executes on one managed goroutine per observed stream so its
deferred work can remain active after `NewStream` returns. gRPC's final status
unwinds the chain on receive completion, cancellation or `Client.Close`, even
when the caller no longer receives. Terminal receive methods wait for the
middleware outcome; normal EOF remains EOF. A successful `CloseSend` only
half-closes the stream and does not end the chain. Receive to completion or
cancel the caller's context. `Client.Close` cancels and joins these executions;
middleware must honor context cancellation and must not call `Close` on its own
client. An unhandled client middleware panic becomes `Internal` (`STREAM_PANIC`);
install recovery middleware for application logging and server handler panics.

Stream options and middleware are available since v1.2.0; v1.1.0 does not
contain them.

## Install

Requires Go 1.27.3. Uses published servex v1.4.0 and kitx v1.0.0.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/transport/grpcx@v1.3.0
```

The example version corresponds to the tag `transport/grpcx/v1.3.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

A server whose `grpc.health.v1` status follows the application readiness, and a
client calling it. Real services register generated code the same way.

```go
package main

import (
	"context"
	"log"

	"github.com/nostosx/servex-contrib/transport/grpcx"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/health"
)

func run() error {
	var a *app.App
	srv := grpcx.NewServer(
		grpcx.WithAddress(":9000"),
		grpcx.WithHealth(health.CheckerFunc(func(ctx context.Context) error { return a.Check(ctx) })),
		// grpcx.WithMiddleware(...) adds servex server middleware for unary calls.
	)
	// pb.RegisterGreeterServer(srv, &greeter{})

	a = app.New(app.WithHooks(srv))
	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	return a.Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

The client side, here with the generated health client:

```go
package main

import (
	"context"
	"log"

	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

func run(ctx context.Context) error {
	// "discovery:///greeter" with grpcx.WithDiscoverer resolves nodes through
	// service discovery instead.
	client, err := grpcx.NewClient(ctx, "grpc://127.0.0.1:9000")
	if err != nil {
		return err
	}
	defer client.Close()

	hc := grpc_health_v1.NewHealthClient(client.Conn())
	resp, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err // an *errorx.Error
	}
	log.Println(resp.GetStatus())
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

Health probes bypass the server middleware, like `/livez` and `/readyz` in
`httpx`. `Server.GetServiceInfo` lets the server be used with
`reflection.Register`.

## Options

Server:

| Option | Default | Description |
|---|---|---|
| `WithName(name)` | `grpc` | Hook name in app logs and errors; give several gRPC servers of one App distinct names. |
| `Address(addr)` | `:9000` | Listen address; `:0` picks a free port reported by `URL()`. |
| `Middleware(m...)` | none | servex server middleware for unary calls, `m[0]` outermost. |
| `GRPCOptions(o...)` | none | Options for the underlying `grpc.Server` (credentials, keepalive, ...). |
| `Health(c)` | not registered | Registers `grpc.health.v1`; status follows `c.Check`. |

Client:

| Option | Default | Description |
|---|---|---|
| `WithDiscoverer(d)` | none | Required for `discovery:///<name>` targets; uses endpoints with the `grpc` scheme. |
| `WithSelector(s)` | `servex-contrib/selector/random.New()` | Node selection per call, for example `selector/p2c`. |
| `WithClientMiddleware(m...)` | none | servex client middleware for unary calls. |
| `WithDialOptions(o...)` | insecure credentials | Dial options for every node connection. |

Calls on a closed client fail with a non-retryable `FailedPrecondition`
(reason `CLIENT_CLOSED`).

## Testing

Unit tests use `bufconn`; there are no integration tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../LICENSE).

## Release

Version v1.3.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/transport/grpcx@v1.3.0
```
