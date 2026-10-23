# config/nacos

`github.com/nostosx/servex-contrib/config/nacos` provides a servex
`config.Source` and `config.Watchable` backed by the
[Nacos](https://github.com/nacos-group/nacos-sdk-go) configuration service.

- A Source reads one configuration item, identified by data ID and group
  (an empty group means `DEFAULT_GROUP`), and decodes it as an object.
- Nacos reports a missing item as empty content, which yields an empty map.
- `Watch` registers a config listener on the client and calls back on every
  change. The client keeps one listener per data ID and group, so do not watch
  the same item through the same client with two Sources at once.
- The client is owned by the caller; the namespace comes from its
  configuration. Client calls cannot be canceled and are bounded by the
  client's own timeout.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Install this module in your application:

```sh
go get github.com/nostosx/servex-contrib/config/nacos@v1.0.0
```

The example version corresponds to the tag `config/nacos/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex-contrib/config/nacos"
	"github.com/nostosx/servex/config"
)

func main() {
	client, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  constant.NewClientConfig(constant.WithTimeoutMs(5000)),
		ServerConfigs: []constant.ServerConfig{*constant.NewServerConfig("127.0.0.1", 8848)},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.CloseClient()

	ctx := context.Background()
	src := nacos.New(client, "user-service.json", nacos.DefaultGroup)
	cfg, err := config.New(ctx, src)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listen on", cfg.String("http.addr"))

	err = cfg.Watch(ctx, func(c *config.Config) {
		log.Println("config changed, log level:", c.String("log.level"))
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

For YAML content pass `nacos.WithCodec(yaml.Codec)` with the codec from
[`encoding/yaml`](../../encoding/yaml), a separate module.

## Options

| Option | Default | Description |
|---|---|---|
| `WithCodec(c)` | `encoding.JSON` | Codec for the content, which must decode to an object. |

Client failures are retryable `errorx.Unavailable`; undecodable content is
`errorx.InvalidArgument`.

## Testing

Unit tests use a fake `IConfigClient`. Integration tests need a Nacos 2 server
in standalone mode:

```sh
SERVEX_IT_NACOS_ADDR=127.0.0.1:8848 GOWORK=off go test -race -count=1 -tags integration ./...
```

`SERVEX_IT_NACOS_ADDR` is the HTTP API address; the gRPC port defaults to the
HTTP port + 1000 and can be set with `SERVEX_IT_NACOS_GRPC_PORT`.

## License

Licensed under the [MIT License](../../LICENSE).
