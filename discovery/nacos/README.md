# discovery/nacos

`github.com/nostosx/servex-contrib/discovery/nacos` implements the servex
`discovery.Registrar` and `discovery.Discoverer` contracts on the
[Nacos](https://github.com/nacos-group/nacos-sdk-go) naming service.

- Every endpoint of an instance is published as an ephemeral Nacos instance
  with the endpoint's host and port, kept alive by the client connection. The
  servex ID, version and endpoint URL are stored in metadata under the
  `servex.` prefix, so an instance needs at least one endpoint with a port.
- `Watch` follows a naming subscription and reports healthy, enabled instances
  with a positive weight in the configured cluster; Nacos instances without
  servex metadata are ignored.
- The naming client must be created with
  `constant.WithUpdateCacheWhenEmpty(true)`. Without it the client ignores
  empty instance lists, and watchers keep reporting the last instances of a
  service that became empty.
- The client is owned by the caller and is not closed by the Registry.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/discovery/nacos@v1.0.0
```

The example version corresponds to the tag `discovery/nacos/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Nacos 2 standalone server with authentication disabled for
local testing. Expose HTTP port `8848` and client gRPC port `9848`. The SDK
writes local cache and logs, so use a writable directory. For an authenticated
server, supply credentials in the SDK client config.

The program starts an HTTP server on port `8000` and registers it as `user`
in group `shop`; Ctrl+C deregisters it and stops the server. No application
routes are registered. Choose an address reachable from consumers before
using this registration across hosts.

```go
package main

import (
	"context"
	"log"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex-contrib/discovery/nacos"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/transport/httpx"
)

func run() error {
	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: constant.NewClientConfig(
			constant.WithTimeoutMs(5000),
			constant.WithUpdateCacheWhenEmpty(true), // required for Watch
		),
		ServerConfigs: []constant.ServerConfig{*constant.NewServerConfig("127.0.0.1", 8848)},
	})
	if err != nil {
		return err
	}
	defer client.CloseClient()

	reg := nacos.New(client, nacos.WithGroup("shop"))

	srv := httpx.NewServer(httpx.Address(":8000"))
	a := app.New(app.WithHooks(srv, discovery.Registration(reg, discovery.Instance{Name: "user", Version: "v1"}, srv)))

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

## Options

| Option | Default | Description |
|---|---|---|
| `WithGroup(s)` | `DEFAULT_GROUP` | Nacos group of registered and watched services. |
| `WithCluster(s)` | `DEFAULT` | Cluster of registered instances; watchers only see this cluster. |

## Testing

Unit tests use a fake `INamingClient`. Integration tests need a Nacos 2 server
in standalone mode:

```sh
SERVEX_IT_NACOS_ADDR=127.0.0.1:8848 GOWORK=off go test -race -count=1 -tags integration ./...
```

`SERVEX_IT_NACOS_ADDR` is the HTTP API address; the gRPC port defaults to the
HTTP port + 1000 and can be set with `SERVEX_IT_NACOS_GRPC_PORT`.

## License

Licensed under the [MIT License](../../LICENSE).
