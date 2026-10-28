# discovery/consul

`github.com/nostosx/servex-contrib/discovery/consul` implements the servex
`discovery.Registrar` and `discovery.Discoverer` contracts on the
[Consul](https://github.com/hashicorp/consul) agent API.

- An instance is registered with the agent as service `Name` with the service
  ID `<escaped-name>-<escaped-id>`, escaping `%` as `%25` and `-` as `%2D`
  in each component, and a TTL check kept passing by a heartbeat every TTL/3.
  When a heartbeat is rejected (for example after an agent restart), the
  instance is registered again.
- `Instance.Metadata` becomes the service meta (keys must satisfy Consul's
  rules); the first endpoint provides the service address and port; ID,
  version and all endpoints are stored in tags prefixed with `servex.`.
- A positive `Instance.Weight` becomes the service's passing weight (warning
  weight 1); other weights keep Consul's defaults. Watched instances carry the
  passing weight, which Consul reports as 1 by default.
- `Watch` follows blocking health queries and reports passing instances only.
- The client is owned by the caller.

## Install

Requires Go 1.27.3. Uses published servex v1.2.0 and kitx v1.0.0 dependencies.

Earlier pre-release checkouts joined unescaped components. If either component
contains `%` or `-`, stop and deregister the old registration before upgrading,
or remove its old service ID explicitly; otherwise it can coexist with the new
registration until its TTL check expires and Consul deregisters it. Monitoring
or automation that stores exact service IDs must use the new escaped ID. Plain
names and IDs without these characters keep their existing IDs.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/discovery/consul@v1.1.0
```

The example version corresponds to the tag `discovery/consul/v1.1.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Consul agent at `127.0.0.1:8500`. The program starts an HTTP
server on port `8000` and registers it as `user`; Ctrl+C deregisters it and
stops the server. No application routes are registered, so this demonstrates
lifecycle and discovery rather than a user endpoint. For a deployment, choose
a listen/advertise address reachable from the service's clients.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex-contrib/discovery/consul"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/transport/httpx"
)

func run() error {
	client, err := api.NewClient(api.DefaultConfig()) // CONSUL_HTTP_ADDR, default 127.0.0.1:8500
	if err != nil {
		return err
	}
	reg := consul.New(client, consul.WithTTL(10*time.Second))

	srv := httpx.NewServer(httpx.WithAddress(":8000"))
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

Clients resolve the service through the same Registry:

```go
client, err := httpx.NewClient(ctx, "discovery:///user", httpx.WithDiscoverer(reg))
```

## Options

| Option | Default | Description |
|---|---|---|
| `WithTTL(d)` | 15s | TTL of the health check; the heartbeat runs every TTL/3. |
| `WithDeregisterCriticalServiceAfter(d)` | 1m | How long Consul keeps an instance whose check is critical before removing it. |

## Testing

Unit tests run against an `httptest` server emulating the Consul agent API.
Integration tests:

```sh
SERVEX_IT_CONSUL_ADDR=127.0.0.1:8500 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
