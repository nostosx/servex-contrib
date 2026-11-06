# ratelimit/redis

Module version for this revision: **v1.0.1**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

A token bucket shared by service replicas.
It uses one atomic Redis script and Redis server time. An idle bucket expires
after its full refill interval. Standalone, Sentinel and Cluster clients are
supported through `redis.UniversalClient`; the application owns the client.
Use identical rates and bursts for every replica sharing a prefix.

The adapter's `AllowContext(ctx, key) (bool, time.Duration, error)` method
structurally satisfies `middleware/endpoint.ContextRateLimiter`.
The modules never import each other.

Assemble them in the application:

```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true,
})
limiter, err := redislimit.New(client, 100, 200,
    redislimit.WithPrefix("orders:caller-quota:"))
if err != nil { return err }
mw := endpointmw.ContextRateLimit(limiter,
    endpointmw.WithRateLimitKey(func(ctx context.Context, req any) string {
        return callerID(ctx) // include tenant and user identity
    }))
```

Here `redis` is `github.com/redis/go-redis/v9`, `redislimit` is this package,
and `endpointmw` is `github.com/nostosx/servex-contrib/middleware/endpoint`.
The fragment assumes an application-provided `callerID` function. Set a caller
deadline, and enable `ContextTimeoutEnabled` on the client so Redis operations
honor it. Client retries follow the injected client's policy.

`ContextRateLimit` distinguishes quota rejection from backend failure. Rejection is
`ResourceExhausted`, with a rounded-up `retry_after_ms` metadata hint. Backend
failure is an error; the middleware rejects by default. Explicit
`endpointmw.WithRateLimitFailOpen(true)` permits calls when the backend fails, but never
ignores cancellation or quota rejection. A closed client is
`FailedPrecondition`, and other backend failures are `Unavailable`.

This module does not add endpoint retries or a local fallback. A Redis request
whose reply is lost may already have spent a token. Choose client retry and
failure policies according to the application's admission requirements.

## Verification

From the repository root:

```sh
task check lint vulncheck \
  MODULES='middleware ratelimit/redis'
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 \
  task test INTEGRATION=true MODULES=ratelimit/redis
```

The integration target requires an explicitly configured disposable Redis,
rejects skipped tests, and checks concurrent clients sharing one quota.

Licensed under MIT. Go 1.27.3 or later is required.
