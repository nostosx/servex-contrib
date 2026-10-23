# middleware

`github.com/nostosx/servex-contrib/middleware` is one module holding the
standard servex middleware. Every package returns an `endpoint.Middleware`,
reads call information from `metadata.FromContext` (kind, protocol,
operation, header) and works with any transport, consumer or worker. The
module depends only on the standard library, servex and kitx; external capabilities
are injected through functions and interfaces.

| Package | Side | Main API | Behaviour |
|---|---|---|---|
| `recovery` | server | `New(WithLogger, WithHandler)` | Turns panics into `errorx.Internal` (reason `PANIC`, not retryable) and logs the stack. |
| `auth` | server, client | `New[P](verify, WithHeader, WithScheme)`, `FromContext[P]`, `Client(token, ...)` | Server: verifies the `Authorization: Bearer` token and stores the principal; missing or invalid tokens are `Unauthenticated`. Client: writes the token into the outbound header. |
| `validate` | server | `New()` | Calls `Validate() error` on requests that implement it (protoc-gen-validate compatible); failures are `InvalidArgument`. |
| `ratelimit` | server | `New(Limiter, WithKey)`, `TokenBucket(rate, burst, WithClock, WithIdleTimeout)` | Rejects with `ResourceExhausted` (reason `RATE_LIMITED`). The token bucket is keyed and evicts idle buckets without a goroutine. |
| `tracing` | both | `New(tracing.Tracer, tracing.Propagator)` | Server spans continue the inbound trace; client spans are injected into the outbound header. |
| `logging` | both | `New(*slog.Logger, WithLevel)` | One record per call with kind, protocol, operation, duration and errorx code. |
| `metrics` | both | `New(metrics.Meter, WithBuckets)` | `servex_requests_total{kind,protocol,operation,code}` and `servex_request_duration_seconds{kind,protocol,operation}`. |
| `retry` | client | `New(WithMaxAttempts, WithBackoff, WithRetryIf)`, `Exponential(base, limit)` | Retries retryable errors (default 3 attempts, exponential backoff with jitter from 100ms to 2s); stops when the caller's context ends. |
| `circuitbreaker` | client | `New(WithKey, WithK, WithWindow, WithBuckets, WithMinRequests, ...)`, `ErrOpen` | Google SRE adaptive throttling per operation; rejects with a non-retryable `Unavailable` (reason `CIRCUIT_OPEN`), so an outer retry stops. |
| `timeout` | client | `New(d)` | Bounds a single attempt; an exceeded deadline is `DeadlineExceeded` (reason `TIMEOUT`). |

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/middleware@v1.0.0
```

The example version corresponds to the tag `middleware/v1.0.0`;
see [module versioning](../README.md#versioning).

## Usage

Middleware runs in the order written; servex never reorders it. The
recommended chains:

```
server:   recovery -> tracing -> logging -> metrics -> auth -> ratelimit -> validate -> handler
client:   tracing -> logging -> metrics -> retry -> circuitbreaker -> timeout -> [select node -> invoke]
consumer: recovery -> tracing -> logging -> metrics -> handler
```

`timeout` inside `retry` bounds each attempt (the overall budget is the
caller's deadline); `circuitbreaker` inside `retry` makes an open breaker end
the retries; `auth` before `ratelimit` allows limiting per caller.

```go
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"time"

	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex-contrib/middleware/logging"
	mwmetrics "github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex-contrib/middleware/recovery"
	"github.com/nostosx/servex-contrib/middleware/retry"
	"github.com/nostosx/servex-contrib/middleware/timeout"
	mwtracing "github.com/nostosx/servex-contrib/middleware/tracing"
	"github.com/nostosx/servex-contrib/middleware/validate"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
	"github.com/nostosx/servex/transport/httpx"
)

type Claims struct{ Subject string }

func verify(_ context.Context, token string) (Claims, error) {
	if token != "secret" {
		return Claims{}, errors.New("unknown token")
	}
	return Claims{Subject: "alice"}, nil
}

func main() {
	logger := slog.Default()
	// Use real backends, for example servex-contrib/tracing/otel and
	// servex-contrib/metrics/prometheus.
	tracer, propagator, meter := tracing.Noop(), tracing.NoopPropagator(), metrics.Noop()

	srv := httpx.NewServer(httpx.Middleware(
		recovery.New(recovery.WithLogger(logger)),
		mwtracing.New(tracer, propagator),
		logging.New(logger),
		mwmetrics.New(meter),
		auth.New(verify),
		ratelimit.New(ratelimit.TokenBucket(100, 200)), // 100 calls/s, bursts of 200
		validate.New(),
	))
	_ = srv // register routes, add to app.App

	client, err := httpx.NewClient(context.Background(), "http://127.0.0.1:8000", httpx.WithClientMiddleware(
		mwtracing.New(tracer, propagator),
		logging.New(logger),
		mwmetrics.New(meter),
		auth.Client(func(context.Context) (string, error) { return "secret", nil }),
		retry.New(retry.WithMaxAttempts(3)),
		circuitbreaker.New(),
		timeout.New(time.Second),
	))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}
```

Handlers read the authenticated principal with `auth.FromContext[Claims](ctx)`.
Each package has a runnable `Example` in its `example_test.go`.

## Testing

```sh
GOWORK=off go test -race -count=1 ./...
GOWORK=off go list -m all   # must list only this module and github.com/nostosx/servex
```

There are no integration tests.

## License

Licensed under the [MIT License](../LICENSE).
