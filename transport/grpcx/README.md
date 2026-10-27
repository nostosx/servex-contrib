# transport/grpcx

Runnable server and client programs: [servex-examples](https://github.com/nostosx/servex-examples).

`github.com/nostosx/servex-contrib/transport/grpcx` implements the servex
transport contracts on [gRPC-Go](https://github.com/grpc/grpc-go), mirroring
the core `transport/httpx`.

- `Server` is a `transport.Server` (an `app.Hook` with a `grpc://` URL) and a
  `grpc.ServiceRegistrar`, so generated code registers directly:
  `pb.RegisterGreeterServer(srv, impl)`. Unary calls run the servex server
  middleware with `metadata.Info{Kind: server, Protocol: "grpc", Operation: full
  method, Header: incoming metadata}`; streaming calls only get the `Info`.
- `Client` exposes `Conn()`, a `grpc.ClientConnInterface` for generated clients
  (`pb.NewGreeterClient(c.Conn())`). Every unary call runs the client
  middleware, selects a node (direct `grpc://host:port` or
  `discovery:///<name>` target) and is made on that node's connection.
  Connections are kept per node and closed once the node leaves the set: the
  client follows node set changes through `transport.WithNodeObserver`, so an
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

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/transport/grpcx@v1.1.0
```

The example version corresponds to the tag `transport/grpcx/v1.0.0`;
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
| `WithSelector(s)` | `selector.Random()` | Node selection per call, for example `selector/p2c`. |
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
