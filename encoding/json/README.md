# encoding/json

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../CONTRIBUTING.md#checks).

Provides `Codec`, an implementation of the core `encoding.Codec` contract
backed by Go's `encoding/json/v2`. Requires the published core contracts from servex v1.7.0.

```go
import jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

data, err := jsoncodec.Codec.Marshal(value)
err = jsoncodec.Codec.Unmarshal(data, &destination)
```

The codec uses the standard library's JSON v2 semantics and is safe for
concurrent use. Treat the exported interface value as read-only. Applications
supply this codec explicitly to contrib APIs such as storage/cache/loader.New; concrete
transport and configuration adapters may choose it as their documented default.
Tests retain JSON v2 behavior, invalid destination, error and concurrency coverage.
