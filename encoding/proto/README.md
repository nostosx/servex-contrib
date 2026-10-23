# encoding/proto

`github.com/nostosx/servex-contrib/encoding/proto` provides `Codec`, a servex
`encoding.Codec` named `proto` that marshals Protocol Buffers messages with
[google.golang.org/protobuf/proto](https://pkg.go.dev/google.golang.org/protobuf/proto).
Values must implement `proto.Message`; anything else yields an
`errorx.InvalidArgument` error. The codec is safe for concurrent use.

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/encoding/proto@v1.0.0
```

The example version corresponds to the tag `encoding/proto/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Register the codec with the core `httpx` server and client. The server picks
it for requests and responses whose media type is `application/x-protobuf`
or `application/proto`; JSON stays available. This example configures the
server and client and runs a direct codec round trip. It does not start the
HTTP server or send a request; register the server as an `app.Hook` to serve
it, as shown in the [core example](https://github.com/nostosx/servex#example).

```go
package main

import (
	"context"
	"log"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nostosx/servex-contrib/encoding/proto"
	"github.com/nostosx/servex/transport/httpx"
)

func main() {
	srv := httpx.NewServer(httpx.Address(":8000"), httpx.Codecs(proto.Codec))
	httpx.Route[wrapperspb.StringValue](srv, "POST /v1/echo", func(_ context.Context, req any) (any, error) {
		return req.(*wrapperspb.StringValue), nil
	})

	// A client that sends and accepts protobuf bodies.
	client, err := httpx.NewClient(context.Background(), "http://127.0.0.1:8000", httpx.WithCodec(proto.Codec))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	_ = httpx.Call[wrapperspb.StringValue](client, "POST", "/v1/echo")

	// The codec can also be used directly.
	data, err := proto.Codec.Marshal(wrapperspb.String("hello"))
	if err != nil {
		log.Fatal(err)
	}
	var v wrapperspb.StringValue
	if err := proto.Codec.Unmarshal(data, &v); err != nil {
		log.Fatal(err)
	}
	log.Println(v.GetValue())
}
```

## Testing

```sh
GOWORK=off go test -race -count=1 ./...
```

There are no integration tests.

## License

Licensed under the [MIT License](../../LICENSE).
