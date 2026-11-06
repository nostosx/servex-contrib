# selector

Module version for this revision: **v1.2.1**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../CONTRIBUTING.md#checks).

`github.com/nostosx/servex-contrib/selector` is one module holding node selection
`selector.Selector` implementations. It depends only on the standard library and
the published servex contracts.

| Package | Constructor | Strategy |
|---|---|---|
| `random` | `New()` | Weighted random selection, with nonpositive weights treated as 1. |
| `roundrobin` | `New()` | Smooth weighted round-robin (the nginx algorithm): picks follow `Node.Weight` exactly over every cycle and are spread evenly instead of in bursts. |
| `p2c` | `New(opts...)` | Power of Two Choices: samples two nodes and picks the one with the lower load, estimated from a peak EWMA of latency and the number of in-flight calls. Outcomes reported through `Done` update the estimate; failures add a latency penalty so failing nodes are avoided until they recover. |

`random.New()` uses `math/rand/v2` and supports concurrent calls. An empty node
set returns `selector.ErrNoAvailable`. Its successful `Done` callback is non-nil
and has no state to update.

All honor `Node.Weight` (values `<= 0` count as 1), which `servex-contrib/transport/client.Client`
copies from `discovery.Instance.Weight` since servex v1.2.0; registries such
as `discovery/consul`, `discovery/etcd` and `discovery/nacos` carry it. Per-node
state follows the node sets passed to `Select`: state of nodes that left the
set is dropped on a later call, so no `client.WithNodeObserver` wiring is
needed.

The contrib shared client requires an explicit `client.WithSelector`.
HTTP/gRPC adapters choose `random.New()` as their contrib default. Because
selection is the innermost step of a client call, every retry selects again
and every outcome is reported to the selector.

## Usage

The complete programs below use the current source API. Use matching peer
sources during development; install real module versions after publication.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex-contrib/selector/p2c"
	"github.com/nostosx/servex-contrib/transport/httpx"
)

func main() {
	reg := memory.New() // any discovery.Discoverer
	client, err := httpx.NewClient(context.Background(), "discovery:///user",
		httpx.WithDiscoverer(reg),
		httpx.WithSelector(p2c.New(p2c.WithDecay(5*time.Second))),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}
```

`roundrobin.New()` is used the same way; both work with any client built on
`servex-contrib/transport/client.Client`, including `grpcx.WithSelector`.

## Options (p2c)

| Option | Default | Description |
|---|---|---|
| `WithDecay(d)` | 2s | Time constant of the latency EWMA. |
| `WithPenalty(d)` | 500ms | Latency recorded for a failed call. |
| `WithClock(fn)` | `time.Now` | Clock, for tests. |
| `WithRandom(fn)` | `math/rand/v2` | Random source, for tests. |

Failures are calls ending with `Unavailable`, `DeadlineExceeded`, `Internal`,
`Unknown` or `ResourceExhausted`.

## Testing

```sh
GOWORK=off go test -race -count=1 ./...
GOWORK=off go list -m all   # only this module and github.com/nostosx/servex
```

Transport/discovery composition regressions live in the discovery/memory
module, keeping this module's dependency boundary limited to core.

## License

Licensed under the [MIT License](../LICENSE).
