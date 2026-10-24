# config/consul

`github.com/nostosx/servex-contrib/config/consul` provides a servex
`config.Source` and `config.Watchable` backed by the
[Consul](https://github.com/hashicorp/consul) KV store.

- Single key mode reads one key holding an encoded document (an object).
- With `WithPrefix()`, every key under the prefix is loaded and merged into one
  tree: with prefix `app/`, the key `app/db/primary` holding `{"dsn":"x"}`
  yields `{"db":{"primary":{"dsn":"x"}}}`.
- `Watch` uses blocking queries and continues from the state read by the
  latest `Load`, so no change is missed. Transient failures are retried with
  exponential delay.
- The client is owned by the caller; datacenter, namespace, partition and token
  come from its configuration.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/config/consul@v1.0.0
```

The example version corresponds to the tag `config/consul/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Consul agent at `127.0.0.1:8500`. Before running the
program, seed its KV prefix with JSON objects:

```sh
curl --fail --request PUT --data '{"addr":"127.0.0.1:8000"}' http://127.0.0.1:8500/v1/kv/services/user/http
curl --fail --request PUT --data '{"level":"info"}' http://127.0.0.1:8500/v1/kv/services/user/log
```

The program prints the configured address and keeps watching until Ctrl+C.
Change the `services/user/log` value to see the callback. These keys are
example data; remove them from your disposable instance after use.

```go
package main

import (
	"context"
	"log"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex-contrib/config/consul"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/config"
)

type HTTPConfig struct {
	Addr string `json:"addr"`
}

func main() {
	client, err := api.NewClient(api.DefaultConfig()) // CONSUL_HTTP_ADDR, default 127.0.0.1:8500
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	cfg, err := config.New(ctx, consul.New(client, "services/user/", consul.WithPrefix()))
	if err != nil {
		log.Fatal(err)
	}
	var httpCfg HTTPConfig
	if err := cfg.Unmarshal("http", &httpCfg); err != nil {
		log.Fatal(err)
	}
	log.Println("listen on", httpCfg.Addr)

	err = cfg.Watch(ctx, func(c *config.Config) {
		log.Println("config changed, log level:", c.String("log.level"))
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

## Options

| Option | Default | Description |
|---|---|---|
| `WithPrefix()` | single key | Treat the key as a prefix and merge every key under it. End it with `/` to avoid matching siblings. |
| `WithCodec(c)` | `encoding.JSON` | Codec for values, for example `servex-contrib/encoding/yaml`. |
| `WithWaitTime(d)` | server default (5m) | Maximum duration of one blocking query (Consul caps it at 10m). |
| `WithRetryDelay(min, max)` | 100ms, 10s | Bounds of the exponential delay between `Watch` retries. |

In single key mode a missing key is `errorx.NotFound`; failures to reach
Consul are retryable `errorx.Unavailable`; undecodable values are
`errorx.InvalidArgument`.

## Testing

Unit tests run against an `httptest` server emulating the Consul HTTP API.
Integration tests:

```sh
SERVEX_IT_CONSUL_ADDR=127.0.0.1:8500 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
