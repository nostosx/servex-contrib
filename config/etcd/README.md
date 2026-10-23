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

Install this module in your application:

```sh
go get github.com/nostosx/servex-contrib/config/etcd@v1.0.0
```

The example version corresponds to the tag `config/etcd/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex-contrib/config/etcd"
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

	ctx := context.Background()
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
	if err != nil {
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
