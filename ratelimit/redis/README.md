# Redis rate limiting

Version v1.0.0 provides a token bucket shared by service replicas.
It uses one atomic Redis script and Redis server time. An idle bucket expires
after its full refill interval. Standalone, Sentinel and Cluster clients are
supported through `redis.UniversalClient`; the application owns the client.
Use identical rates and bursts for every replica sharing a prefix.

The adapter's `AllowContext(ctx, key) (bool, time.Duration, error)` method
structurally satisfies `middleware/ratelimit.ContextLimiter` (middleware v1.3.0).
The modules never import each other. Install the two independent modules in
your application:

```sh
go get github.com/nostosx/servex-contrib/ratelimit/redis@v1.0.0
go get github.com/nostosx/servex-contrib/middleware@v1.3.0
```

Assemble them in the application:

```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true,
})
limiter, err := redislimit.New(client, 100, 200,
    redislimit.WithPrefix("orders:caller-quota:"))
if err != nil { return err }
mw := ratelimit.NewContext(limiter,
    ratelimit.WithKey(func(ctx context.Context, req any) string {
        return callerID(ctx) // include tenant and user identity
    }))
```

Here `redis` is `github.com/redis/go-redis/v9`, `redislimit` is this package,
and `ratelimit` is `github.com/nostosx/servex-contrib/middleware/ratelimit`.
The fragment assumes an application-provided `callerID` function. Set a caller
deadline, and enable `ContextTimeoutEnabled` on the client so Redis operations
honor it. Client retries follow the injected client's policy.

`NewContext` distinguishes quota rejection from backend failure. Rejection is
`ResourceExhausted`, with a rounded-up `retry_after_ms` metadata hint. Backend
failure is an error; the middleware rejects by default. Explicit
`ratelimit.WithFailOpen(true)` permits calls when the backend fails, but never
ignores cancellation or quota rejection. A closed client is
`FailedPrecondition`, and other backend failures are `Unavailable`.

This module does not add endpoint retries or a local fallback. A Redis request
whose reply is lost may already have spent a token. Choose client retry and
failure policies according to the application's admission requirements.

## Verification

From the repository root:

```sh
MODULES='middleware ratelimit/redis' task check lint vulncheck
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='ratelimit/redis' task test INTEGRATION=true
```

The integration target requires an explicitly configured disposable Redis,
rejects skipped tests, and checks concurrent clients sharing one quota.

Licensed under MIT. Go 1.27.3 or later is required.
