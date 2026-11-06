# transport/client

Module version for this revision: **v1.1.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

Shared client execution over the core transport, discovery and selector contracts.
This module is SDK-free and is used by both transport/httpx and transport/grpcx.
Requires published servex v1.7.0.

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

Endpoint installs client call metadata and runs logical-call middleware in
its declared order. The engine then executes the configured Policy, selecting a
node and reporting the result through selector.Done for each native attempt.
The engine does not own protocol connections or the injected selector.

## Call and attempt policies

`WithPolicy` selects a default `Policy`; `WithOperationPolicies` replaces it for
exact stable operation names. Configure `Timeout`, `AttemptTimeout`, `MaxAttempts`,
`RetrySafe`, `RetryIf` and `Backoff` here. A zero policy makes one attempt with the
caller deadline. Multiple attempts require both explicit replay safety and a
retry predicate. The engine is the only retry owner; the removed generic retry
and timeout Endpoint middleware must not be recreated around it.

HTTP and gRPC expose these policies as `WithCallPolicy` and
`WithOperationPolicies`. `WithMiddleware` observes the logical call, while
`WithAttemptMiddleware` wraps the selected-node invocation. Applications import
`middleware/client` once and choose Logging, Metrics and Tracing for logical calls,
or AttemptLogging, AttemptMetrics and AttemptTracing for selected-node invocations. Inbound factories live under
middleware/endpoint; typed business factories live under middleware/application.
The `Attempt` adapter can also apply an explicitly chosen client policy, such as
`middleware/client.CircuitBreaker`, at that native boundary. A per-node circuit
breaker requires an explicit node key; the adapter must never replay next.

Operation policy maps are captured during construction. Configuration watching
does not automatically replace a running client's policy. Streams retain their
own lifetime and are never replayed by unary policies.

WithNodeObserver reports changes in Addr, Weight and Metadata. Its initial call
finishes before New returns; subsequent calls are serialized with discovery
updates and receive deep copies. Observers must return promptly and must not
call Close. No observation occurs after Close returns.

Close rejects new calls and retry attempts with non-retryable
FailedPrecondition/CLIENT_CLOSED, then stops and joins the discovery watcher.
It is idempotent. Running invocations obey their contexts; protocol adapters
own connection shutdown. Closing during selection still reports its outcome
through selector.Done without invoking the transport. A permissive RetryIf
cannot replay the terminal closed error.

## Validation

Use repository Task gates with matching sources: task check, task lint and
task vulncheck, filtered with MODULES='transport/client'. This module depends
only on core, kitx/backoff and the standard library, including its tests.
Discovery/memory owns the existing backend composition and concurrency
regressions; HTTP/gRPC tests cover real protocol roundtrips over the engine.
Do not commit local replacements or claim public-proxy acceptance before release.
