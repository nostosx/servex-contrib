# cache/loader

`github.com/nostosx/servex-contrib/cache/loader` adds typed read-through
loading and duplicate-load suppression to the core `cache.Cache` contract.
The backing cache and codec are supplied explicitly. No codec is selected by
default.

This module is unreleased. Its initial version baseline is v1.0.0, with Go
1.27.3, servex v1.3.0 and kitx v1.0.0 dependencies. Validate the matching
unreleased core and contrib sources with temporary module overrides before a
coordinated release; keep local replacements out of application manifests.

## Usage

```go
import (
    "context"
    "time"

    cacheloader "github.com/nostosx/servex-contrib/cache/loader"
    "github.com/nostosx/servex/encoding"
    "github.com/nostosx/servex/storage/cache"
)

func newUsers(owner context.Context, backend cache.Cache, codec encoding.Codec) (*cacheloader.Loader[string], error) {
    return cacheloader.New[string](
        owner,
        backend,
        5*time.Second,
        cacheloader.WithCodec(codec),
        cacheloader.WithTTL(time.Minute),
    )
}
```

Call `Get(ctx, key, load)` to decode a cached value or share one load per key.
Call `Close` to stop admission, cancel and join shared work; it does not close
the backing cache. A non-positive execution timeout returns
`ErrInvalidLoadTimeout`; a missing codec returns `ErrCodecRequired`. A closed
or zero-value Loader returns `ErrLoaderClosed`.

Shared executions inherit the constructor's owner context and never the
initiating request's values. Each waiter can cancel independently. Keys must
include tenant and authorization scope. Same-key callbacks must be
interchangeable, and callers must treat shared pointer, slice and map results
as immutable. Applications must bound the number of distinct active keys.
Callbacks and backends must honor cancellation. A callback must not call
`Close` or reenter its own key.

Cache errors fall through to loading, but errors other than `cache.ErrNotFound`
disable write-back. Decode errors trigger a reload. Encoding and write-back
errors do not replace successful load results. The execution timeout covers
rechecking, loading, encoding and write-back; synchronous codecs cannot be
interrupted, and cancellation cannot undo a write already in progress.

## Testing

The regression suite covers cache hits and misses, codec behavior, concurrent
loads, independent waiter cancellation, owner values, timeouts and shutdown.

```sh
GOWORK=off go test -race -shuffle=on -count=1 ./...
```

## License

Licensed under the [MIT License](LICENSE).
