# transport/grpcx

Module version for this revision: **v1.5.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

Runnable server and client programs: [servex-examples](https://github.com/nostosx/servex-examples).

`github.com/nostosx/servex-contrib/transport/grpcx` implements the servex
transport contracts on [gRPC-Go](https://github.com/grpc/grpc-go), mirroring
the sibling `transport/httpx`.

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
  Policy may retry them, but a server returning such an error, directly
  or wrapped, sends `PropagatedRetryable`, so its caller does not retry it
  again. Set `errorx.From(err).WithRetryable(true)` in the handler to let the
  caller retry. Statuses gRPC reports for connection failures are remote too;
  `Canceled` and `DeadlineExceeded` caused by the caller's own context are not.

## File configuration

`ServerConfig` is a data-only model for configuration decoding. Initialize it
with `DefaultServerConfig()` before applying file/environment values, then call
`Validate()` or `NewServerFromConfig(config, runtimeOptions...)`. Construction
opens no listener; register services before `Start`.

```go
cfg := grpcx.DefaultServerConfig()
if err := settings.Unmarshal("grpc", &cfg); err != nil { return err }
server, err := grpcx.NewServerFromConfig(cfg,
    grpcx.WithHealth(application),
    grpcx.WithMiddleware(authenticate),
)
if err != nil { return err }
```

Here `settings` is a core config.Reader supplied by the application. Viper's
duration decoder accepts strings such as `"5s"`:

```toml
[grpc]
name = "grpc"
address = ":9000"
max_recv_msg_size = 4194304
max_send_msg_size = 2147483647
health_interval = "5s"
```

Defaults match name/address and health cadence of `NewServer`. Zero message
limits select the gRPC defaults (4 MiB receive, 2147483647 bytes send), rather
than disabling all messages. Negative or oversized limits, nonpositive health
intervals, blank names and malformed/nonnumeric host:port addresses are rejected.
Port zero is valid for ephemeral listeners. Health interval affects only the
health service installed with `WithHealth`.

Middleware, credentials, listeners and health checkers remain runtime options.
Scalar overrides are validated before construction. Configured message limits
are applied after `WithGRPCOptions`, so native SDK options cannot silently
replace them. Use the low-level `NewServer(options...)` constructor when native
SDK options should control these limits directly. File loading, reload and
resource replacement belong to the application.


## Streaming middleware

`WithStreamMiddleware` (server) and `WithClientStreamMiddleware` (client)
accept servex `endpoint.Middleware`. Each chain runs once around the complete
streaming RPC, including bidirectional and client-streaming calls. The request
is a `*grpcx.StreamInfo`, with `ClientStreams` / `ServerStreams` flags and
concurrent-safe `SentMessages()` / `ReceivedMessages()` counters. Treat the
flags as read-only. Counters count successful message operations on this side
of the connection and exclude EOF and failed operations; message contents are
never passed through the call-level middleware.

The following assembly fragments use separate server and client packages.
Aliases identify the execution boundary when both are assembled in one file:

```go
import (
    clientmw "github.com/nostosx/servex-contrib/middleware/client"
    endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
    "github.com/nostosx/servex-contrib/transport/grpcx"
)

srv := grpcx.NewServer(grpcx.WithStreamMiddleware(
    endpointmw.Tracing(tracer, propagator),
    endpointmw.Logging(logger),
    endpointmw.Metrics(meter),
    endpointmw.Recovery(),
    endpointmw.Authentication(verify),
))

client, err := grpcx.NewClient(ctx, target, grpcx.WithClientStreamMiddleware(
    clientmw.Tracing(tracer, propagator),
    clientmw.Logging(logger),
    clientmw.Metrics(meter),
    clientmw.Authentication(token),
))
```

The same scope packages apply to unary WithMiddleware/WithClientMiddleware.
Unary WithClientAttemptMiddleware uses clientmw.AttemptLogging,
clientmw.AttemptMetrics and clientmw.AttemptTracing from that same client package. Application
factories remain under middleware/application and wrap the business handler.

Unary middleware remains separate. Authentication runs when the stream starts;
logging, request metrics and spans cover its full lifetime. To record message
counts, custom middleware can read the `StreamInfo` counters after `next`
returns. Per-message validation belongs in the handler or native gRPC stream
interceptors. Health Watch bypasses the server stream chain, like unary probes.

A stream must not be replayed. Do not add unary retry, request validation or
unary attempt budgets to this chain. A second call to `next` is rejected
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

## Usage

The complete programs below use the current source API. Use matching peer
sources during development; install real module versions after publication.

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


## v1.5.0: authenticated health probes

WithHealthMiddleware explicitly applies the configured unary chain to health
Check and the stream chain to health Watch. Default behavior continues to bypass
both chains. Applications select auth/recovery/logging policies and their order
at their own entry points. Avoid whole-stream deadlines on long-lived health
Watch RPCs unless intentional.

WithPublicErrors is a separate opt-in handler-error policy. It preserves native
gRPC status codes and application error codes while replacing messages with
canonical reasons and removing status details. Unknown handler errors become
Internal. Middleware observes the original error before this wire projection.
