# config/etcd

`github.com/nostosx/servex-contrib/config/etcd` provides a servex
`config.Source` and `config.Watchable` backed by
[etcd](https://github.com/etcd-io/etcd) v3.

- Single key mode reads one key holding an encoded document (an object).
- With `WithPrefix()`, every key under the prefix is loaded and merged into one
  tree: with prefix `/app/`, the key `/app/db/primary` holding `{"dsn":"x"}`
  yields `{"db":{"primary":{"dsn":"x"}}}`.
- `Watch` resumes right after the revision read by the latest `Load`, so no
  change is missed; all events of one transaction produce one notification.
- The client is owned by the caller and never closed by the Source.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/config/etcd@v1.0.0
```

The example version corresponds to the tag `config/etcd/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated etcd v3 server at `127.0.0.1:2379` and seed the example's
prefix with JSON objects using `etcdctl`:

```sh
etcdctl --endpoints=127.0.0.1:2379 put /config/user/http '{"addr":"127.0.0.1:8000"}'
etcdctl --endpoints=127.0.0.1:2379 put /config/user/log '{"level":"info"}'
```

The program prints the configured address and keeps watching until Ctrl+C.
Change `/config/user/log` to see the callback. Later sources override earlier
ones, so etcd values take precedence over `USER_` environment variables here.
Remove the example prefix from your disposable instance after use.

```go
package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex-contrib/config/etcd"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/config"
)

func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	// Later sources override earlier ones.
	cfg, err := config.New(ctx,
		config.Env("USER_"),
		etcd.New(client, "/config/user/", etcd.WithPrefix()),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listen on", cfg.String("http.addr"))

	// Reload and react to changes until ctx ends.
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

In single key mode a missing key is `errorx.NotFound`; failures to reach etcd
are retryable `errorx.Unavailable`; undecodable values are
`errorx.InvalidArgument`.

## Testing

Unit tests run against an embedded etcd server. Integration tests:

```sh
SERVEX_IT_ETCD_ENDPOINTS=127.0.0.1:2379 GOWORK=off go test -race -count=1 -tags integration ./...
```

`SERVEX_IT_ETCD_ENDPOINTS` is comma separated.

## License

Licensed under the [MIT License](../../LICENSE).
