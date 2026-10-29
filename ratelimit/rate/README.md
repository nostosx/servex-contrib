# ratelimit/rate

Keyed local token buckets backed by `golang.org/x/time/rate`. The independent
module implements the existing structural `Allow(key string) bool` Limiter
contract. Core and generic middleware do not depend on x/time. This module is
unreleased; use matching source overrides outside repository manifests for
development validation.

## Usage

```go
package main

import (
    "context"
    "fmt"

    mwratelimit "github.com/nostosx/servex-contrib/middleware/ratelimit"
    "github.com/nostosx/servex-contrib/ratelimit/rate"
)

func main() {
    limiter, err := rate.New(100, 200, rate.WithMaxKeys(10000))
    if err != nil {
        panic(err)
    }
    handler := mwratelimit.New(limiter, mwratelimit.WithKey(func(_ context.Context, req any) string {
        return req.(string) // derive a trusted tenant/principal key in your application
    }))(func(context.Context, any) (any, error) {
        return "ok", nil
    })
    result, err := handler(context.Background(), "tenant-a")
    fmt.Println(result, err)
}
```

Each key starts with burst tokens and replenishes at the configured rate per
second. Allow never waits or reserves future tokens. Instances and replicas
have independent quotas; use a distributed backend when quotas must be shared.
Generic middleware maps rejection to ResourceExhausted/RATE_LIMITED as before.

## Bounds and lifecycle

| Option | Default | Contract |
|---|---|---|
| WithMaxKeys(n) | 10000 | Positive retained-key limit; unknown keys are rejected at capacity. |
| WithIdleTimeout(d) | 1 minute | Positive minimum idle time and lazy sweep interval. |
| WithClock(fn) | time.Now | Test clock; called under the limiter lock, must not reenter. |

Only idle, fully replenished buckets are reclaimed. A partially spent bucket
is never removed to admit a new key, so key churn cannot reset another key's
quota. Rejected calls to an existing key count as activity. Cleanup runs on
Allow at most once per idle interval; no goroutine or Close is required.
Sweeps may cost time proportional to the bounded number of keys. Select key
limits and idle intervals for your application's cardinality and use trusted,
bounded key dimensions when possible.

New rejects nonpositive/nonfinite rates, nonpositive bursts and invalid options
with InvalidArgument/RATE_LIMIT_CONFIG. Calls are concurrency-safe. The zero
value and a nil *Limiter fail closed. This adapter is an optional implementation;
the existing SDK-free middleware/ratelimit.TokenBucket remains available.

## Checks

Use `MODULES='ratelimit/rate' task check lint vulncheck` with matching core and
middleware sources. Regression tests use a fixed clock for admission/refill,
exercise key-capacity and slow-refill reclamation, verify the actual generic
middleware composition and bound concurrent admissions to the configured burst.

## License

[MIT](LICENSE); golang.org/x/time uses BSD-3-Clause.
