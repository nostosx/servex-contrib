# selector

`github.com/nostosx/servex-contrib/selector` is one module holding stateful
`selector.Selector` implementations. It depends only on the standard library,
servex and its kitx dependency.

| Package | Constructor | Strategy |
|---|---|---|
| `roundrobin` | `New()` | Smooth weighted round-robin (the nginx algorithm): picks follow `Node.Weight` exactly over every cycle and are spread evenly instead of in bursts. |
| `p2c` | `New(opts...)` | Power of Two Choices: samples two nodes and picks the one with the lower load, estimated from a peak EWMA of latency and the number of in-flight calls. Outcomes reported through `Done` update the estimate; failures add a latency penalty so failing nodes are avoided until they recover. |

Both honor `Node.Weight` (values `<= 0` count as 1), which `transport.Client`
copies from `discovery.Instance.Weight` since servex v1.2.0; registries such
as `discovery/consul`, `discovery/etcd` and `discovery/nacos` carry it. Per-node
state follows the node sets passed to `Select`: state of nodes that left the
set is dropped on a later call, so no `transport.WithNodeObserver` wiring is
needed.

The core default is `selector.Random()` (weighted random). Because node
selection is the innermost step of a client call, every retry selects again
and every outcome is reported to the selector.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.
Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/selector@v1.1.0
```

The example version corresponds to the tag `selector/v1.0.0`;
see [module versioning](../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/nostosx/servex-contrib/selector/p2c"
	"github.com/nostosx/servex/discovery/memory"
	"github.com/nostosx/servex/transport/httpx"
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
`transport.Client`, including `grpcx.WithSelector`.

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
GOWORK=off go list -m all   # only this module, github.com/nostosx/servex and github.com/nostosx/kitx
```

There are no integration tests.

## License

Licensed under the [MIT License](../LICENSE).
