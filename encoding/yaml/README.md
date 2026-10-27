# encoding/yaml

`github.com/nostosx/servex-contrib/encoding/yaml` provides `Codec`, a servex
`encoding.Codec` named `yaml` backed by
[go.yaml.in/yaml/v3](https://github.com/yaml/go-yaml).

Mappings decoded into dynamically typed values (`any`, `map[string]any`,
`[]any`) are always `map[string]any`: non-string keys are formatted with
`fmt.Sprint` and timestamps become RFC 3339 strings. Decoded trees therefore
merge with those of other configuration sources. The codec is safe for
concurrent use.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/encoding/yaml@v1.1.0
```

The example version corresponds to the tag `encoding/yaml/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

The core `config/file` source only maps `.json` by default; register the YAML
codec for the extensions you use:

Save this configuration as `config.yaml` in the directory where you run the
program:

```yaml
http:
  addr: "127.0.0.1:8000"
```

Save the following program as `main.go`, then run `go mod tidy` and
`go run .`. It prints `listen on 127.0.0.1:8000` and exits; it reads the
configuration without starting an HTTP server. `APP_HTTP__ADDR`, when set,
overrides the file value.

```go
package main

import (
	"context"
	"log"

	"github.com/nostosx/servex-contrib/encoding/yaml"
	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/config/file"
)

func main() {
	src := file.New("config.yaml",
		file.WithCodec(".yaml", yaml.Codec),
		file.WithCodec(".yml", yaml.Codec),
	)
	cfg, err := config.New(context.Background(), []config.Source{src, config.Env("APP_")})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listen on", cfg.String("http.addr"))
}
```

The same codec works with the `WithCodec` options of
[`config/etcd`](../../config/etcd), [`config/consul`](../../config/consul) and
[`config/nacos`](../../config/nacos), and with `httpx.Codecs` for
`application/yaml` bodies.

## Testing

```sh
GOWORK=off go test -race -count=1 ./...
```

There are no integration tests.

## License

Licensed under the [MIT License](../../LICENSE).
