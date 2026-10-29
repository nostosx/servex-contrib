# transport/client

Shared client execution over the core transport, discovery and selector contracts.
This module is SDK-free and is used by both transport/httpx and transport/grpcx.
It is unreleased and requires the matching core/contrib sources through external
temporary modfiles until a coordinated release.

## Migration

Replace core transport.Client with client.Client, transport.NewClient with
client.New and transport.ClientOption with client.Option. WithDiscoverer,
WithSelector, WithScheme, WithMiddleware and WithNodeObserver move to this
package. Core retains transport.Server, transport.Invoker and InvokerFunc.
There are no forwarding aliases from core to contrib.

When building a protocol adapter, construct the engine with an explicit selector:

```go
import (
    sxclient "github.com/nostosx/servex-contrib/transport/client"
    "github.com/nostosx/servex-contrib/selector/random"
)

c, err := sxclient.New(ctx, "discovery:///users",
    sxclient.WithDiscoverer(registry),
    sxclient.WithScheme("http"),
    sxclient.WithSelector(random.New()),
)
if err != nil {
    return err
}
defer c.Close()
call := c.Endpoint("/users/Get", invoker, middleware...)
```

The registry implements discovery.Discoverer and invoker implements
transport.Invoker. Direct targets use `<scheme>://host:port` and need no registry.
Applications using HTTP or gRPC should normally construct the owning transport
adapter rather than wire this engine themselves. Those adapters choose their
contrib defaults; this engine never selects an implicit algorithm.

## Execution and ownership

For discovery targets, New waits for the first snapshot within the caller's
context. A failed first snapshot fails construction. Later watch failures keep
the last known nodes while recovery uses exponential backoff from 100ms to 5s.
Only endpoints matching the configured scheme enter the node set; weights and
metadata are copied from each discovery instance.

Endpoint installs client call metadata, runs middleware in declaration order,
selects a node and invokes the protocol adapter. Selection is inside middleware,
so every retry selects again and reports its result through selector.Done.
The engine does not own protocol connections or the injected selector.

WithNodeObserver reports changes in Addr, Weight and Metadata. Its initial call
finishes before New returns; subsequent calls are serialized with discovery
updates and receive deep copies. Observers must return promptly and must not
call Close. No observation occurs after Close returns.

Close stops and joins the discovery watcher and is idempotent. Existing endpoints
retain the last known node set; protocol adapters control connection shutdown
and rejection of calls after their own Close. All prior behavior is preserved.

## Validation

Use repository Task gates with matching sources: task check, task lint and
task vulncheck, filtered with MODULES='transport/client'. This module depends
only on core, kitx/backoff and the standard library, including its tests.
Discovery/memory owns the existing backend composition and concurrency
regressions; HTTP/gRPC tests cover real protocol roundtrips over the engine.
Do not commit local replacements or claim public-proxy acceptance before release.
