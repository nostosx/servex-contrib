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

Requires Go 1.27.3. Uses published servex v1.4.0.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/config/nacos@v1.2.0
```

The example version corresponds to the tag `config/nacos/v1.2.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Nacos 2 standalone server with authentication disabled
for local testing. Expose HTTP port `8848` and client gRPC port `9848`.
In its console, create data ID `user-service.json` in `DEFAULT_GROUP` in
the public namespace with this JSON content:

```json
{"http":{"addr":"127.0.0.1:8000"},"log":{"level":"info"}}
```

The program prints the configured address and keeps watching until Ctrl+C.
Publish a changed log level to see the callback, and delete the example item
after use. The SDK writes local cache and logs; run it in a writable directory.
For an authenticated server, supply credentials in the SDK client config.

```go
package main

import (
	"context"
	"log"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex-contrib/config/nacos"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/config"
 cfgviper "github.com/nostosx/servex-contrib/config/viper"
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

	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	src := nacos.New(client, "user-service.json", nacos.DefaultGroup)
	cfg, err := cfgviper.New(ctx, []config.Source{src})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listen on", cfg.String("http.addr"))

	// Watch options such as WithReloadErrorHandler follow the callback.
	err = cfg.Watch(ctx, func(c config.Config) {
		log.Println("config changed, log level:", c.String("log.level"))
	}, cfgviper.WithReloadErrorHandler(func(err error) {
		log.Println("config reload failed, keeping previous values:", err)
	}))
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

For YAML content pass `nacos.WithCodec(yaml.Codec)` with the codec from
[`encoding/yaml`](../../encoding/yaml), a separate module.

## Options

| Option | Default | Description |
|---|---|---|
| `WithCodec(c)` | `servex-contrib/encoding/json.Codec` | Codec for the content, which must decode to an object. |

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

Configuration-engine examples use config/viper v1.0.0 and servex v1.4.0.
Earlier releases keep their historical API.

## Release

Version v1.2.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/config/nacos@v1.2.0
```
