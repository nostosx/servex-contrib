# encoding/json

Provides `Codec`, an implementation of the core `encoding.Codec` contract
backed by Go's `encoding/json/v2`. This adapter is unreleased and requires
the matching core source revision until a coordinated release.

```go
import jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

data, err := jsoncodec.Codec.Marshal(value)
err = jsoncodec.Codec.Unmarshal(data, &destination)
```

The codec uses the standard library's JSON v2 semantics and is safe for
concurrent use. Treat the exported interface value as read-only. Applications
supply this codec explicitly to contrib APIs such as cache/loader.New; concrete
transport and configuration adapters may choose it as their documented default.
Tests retain JSON v2 behavior, invalid destination, error and concurrency coverage.
