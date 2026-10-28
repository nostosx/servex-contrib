# discovery/etcd

`github.com/nostosx/servex-contrib/discovery/etcd` implements the servex
`discovery.Registrar` and `discovery.Discoverer` contracts on
[etcd](https://github.com/etcd-io/etcd) v3.

- An instance is stored as JSON under `<namespace>/<name>/<id>`, attached to a
  lease that is kept alive while it is registered. A lost lease (for example
  after a network partition) is granted again and the instance rewritten.
- A nonzero `Instance.Weight` is stored in the `weight` field and reported to
  watchers, so weighted selectors see it; older values without the field read
  as weight 0, the selector default.
- `Watch` returns the full instance list first and then follows changes with an
  etcd watch; interrupted watches, including compactions, recover by reading
  the full list again.
- The client is owned by the caller and never closed by the Registry.

## Install

Requires Go 1.27.3. Uses published servex v1.2.0 and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/discovery/etcd@v1.1.0
```

The example version corresponds to the tag `discovery/etcd/v1.1.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated etcd v3 server at `127.0.0.1:2379`. The program starts an
HTTP server on port `8000` and registers it as `user`; Ctrl+C removes its
registration and stops the server. No application routes are registered.
For a deployment, choose an address reachable from the service's clients.

```go
package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex-contrib/discovery/etcd"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/transport/httpx"
)

func run() error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	reg := etcd.New(client, etcd.WithTTL(10*time.Second))

	srv := httpx.NewServer(httpx.WithAddress(":8000"))
	// Registration registers the server URL after it listens and
	// deregisters it before it stops.
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
| `WithNamespace(ns)` | `/servex/services` | Key prefix under which instances are stored. |
| `WithTTL(d)` | 15s | Lease TTL, rounded up to whole seconds. |

## Testing

Unit tests run against an embedded etcd server. Integration tests:

```sh
SERVEX_IT_ETCD_ENDPOINTS=127.0.0.1:2379 GOWORK=off go test -race -count=1 -tags integration ./...
```

`SERVEX_IT_ETCD_ENDPOINTS` is comma separated; each test uses its own
namespace.

## License

Licensed under the [MIT License](../../LICENSE).
