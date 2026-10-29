# middleware

`github.com/nostosx/servex-contrib/middleware` is one module holding the
standard servex middleware. Each package provides `endpoint.Middleware` via `New`,
which reads call information from `metadata.FromContext` (kind, protocol,
operation, header) and works with any transport, consumer or worker. The
module depends only on the standard library, servex, kitx and the SDK-free
metrics/noop and tracing/noop backends. External capabilities are injected
through functions and interfaces.

| Package | Side | Main API | Behaviour |
|---|---|---|---|
| `recovery` | server | `New(WithLogger, WithHandler)` | Turns panics into `errorx.Internal` (reason `PANIC`, not retryable) and logs the stack. |
| `auth` | server, client | `New[P](verify, WithHeader, WithScheme)`, `FromContext[P]`, `Client(token, ...)` | Server: verifies the `Authorization: Bearer` token and stores the principal; missing or invalid tokens are `Unauthenticated`. Client: writes the token into the outbound header. |
| `validate` | server | `New()` | Calls `Validate() error` on requests that implement it (protoc-gen-validate compatible); failures are `InvalidArgument`. |
| `ratelimit` | server | `New(Limiter, WithKey)`, `TokenBucket(rate, burst, WithClock, WithIdleTimeout)` | Rejects with `ResourceExhausted` (reason `RATE_LIMITED`). The token bucket is keyed and evicts idle buckets without a goroutine. |
| `tracing` | both | `New(tracing.Tracer, tracing.Propagator)`, `IsServerFault` | Server spans continue the inbound trace; client spans are injected into the outbound header. Following OpenTelemetry, server and internal spans get an error status only for server faults (Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss); client spans for every failure. The `servex.code` attribute is always set. |
| `logging` | both | `New(logger.Logger, WithLevel)` | One record per call with kind, protocol, operation, duration and errorx code. |
| `metrics` | both | `New(metrics.Meter, WithBuckets)` | `servex_requests_total{kind,protocol,operation,code}` and `servex_request_duration_seconds{kind,protocol,operation}`. |
| `retry` | client | `New(WithMaxAttempts, WithBackoff, WithRetryIf)`, `Exponential(base, limit)` | Retries retryable errors (default 3 attempts, exponential backoff with jitter from 100ms to 2s); stops when the caller's context ends. A remote error is retried only by the hop that received it: servex servers report it to their callers as not retryable. |
| `circuitbreaker` | client | `New(WithKey, WithK, WithWindow, WithBuckets, WithMinRequests, ...)`, `ErrOpen` | Google SRE adaptive throttling per operation; rejects with a non-retryable `Unavailable` (reason `CIRCUIT_OPEN`), so an outer retry stops. |
| `timeout` | client | `New(d)` | Bounds a single attempt; an exceeded deadline is `DeadlineExceeded` (reason `TIMEOUT`). |

## Distributed rate limiting and gRPC streams

Since middleware v1.3.0, `ratelimit.NewContext` accepts a `ContextLimiter` with caller context, a
retry-delay hint and backend errors. It supports distributed adapters such as
`servex-contrib/ratelimit/redis` without a cross-module import. Backend errors
reject by default; `WithFailOpen(true)` explicitly admits calls on a backend
failure. Caller cancellation and quota rejection are never bypassed. `New`
and the local `TokenBucket` remain unchanged.

For gRPC streams, explicitly assemble `grpcx.WithStreamMiddleware` or
`grpcx.WithClientStreamMiddleware`; the unary chain does not apply to streams.
See the grpcx README for lifecycle and retry constraints.

## Typed application middleware

Version v1.1.0 also provides `logging.Command[C,R]` / `Query[Q,R]`,
`metrics.Command[C,R]` / `Query[Q,R]` and `tracing.Command[C,R]` / `Query[Q,R]`.
These use servex/cqrs v1.1.0;
**middleware/v1.0.0 does not contain these APIs**. Keep the endpoint API above
for transport calls. Application wrappers consume `cqrs.Metadata{Kind, Operation}`
from `ChainCommand` / `ChainQuery`, independently of transport metadata.

Assemble once in the application composition root, with stable operation names:

```go
handler := cqrs.ChainCommand("catalog.create_product", command,
    tracing.Command[Create, Product](tracer),
    metrics.Command[Create, Product](meter),
    logging.Command[Create, Product](logger),
)
```

This is an assembly fragment; request/result types, command and sinks belong to
the application. For a complete command/query program see the executable
[ExampleCommand](logging/cqrs_test.go). The first wrapper is outermost; logging
runs inside the internal span for trace correlation. Nil loggers disable logging
and no-op metrics/tracing; WithLevel and WithBuckets apply to the typed factories.
Use concurrency-safe sinks and handlers. No automatic retry or transaction is added.

- Logs: kind, operation, duration, code, and trace/span IDs when valid.
- Metrics: `servex_usecase_calls_total{kind,operation,code}` and
  `servex_usecase_duration_seconds{kind,operation}`; bounded operation names only.
- Traces: internal child spans named after the operation; kind, operation and code.
  Caller context is preserved through the child context; no transport headers are
  extracted, injected or changed.

Requests, results, error text, error reasons and panic values are excluded from
these signals. Spans always carry the errorx code (`Unknown` for unclassified
errors); only server faults record a code-only error and an error status, so
client-class results such as NotFound or InvalidArgument do not. Panics count as `Internal`, end the span once and propagate unchanged;
recovery belongs at the outer transport/process boundary. Results and original
errors pass back unchanged, including nonzero results returned together with errors.

## Install

Requires Go 1.27.3 or later. Version v1.3.0 uses published servex v1.3.0
and kitx v1.0.0, without local replacements.
Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/middleware@v1.4.0
```

The example version corresponds to the tag `middleware/v1.3.0`;
see [module versioning](../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

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

	slogadapter "github.com/nostosx/servex-contrib/logger/slog"
	metricsnoop "github.com/nostosx/servex-contrib/metrics/noop"
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
	tracingnoop "github.com/nostosx/servex-contrib/tracing/noop"
	"github.com/nostosx/servex-contrib/transport/httpx"
)

type Claims struct{ Subject string }

func verify(_ context.Context, token string) (Claims, error) {
	if token != "secret" {
		return Claims{}, errors.New("unknown token")
	}
	return Claims{Subject: "alice"}, nil
}

func main() {
	logger := slogadapter.New(slog.Default())
	// Use real backends, for example servex-contrib/tracing/otel and
	// servex-contrib/metrics/prometheus.
	tracer, propagator, meter := tracingnoop.New(), tracingnoop.NewPropagator(), metricsnoop.New()

	srv := httpx.NewServer(httpx.WithMiddleware(
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
GOWORK=off go list -m all   # only this module, github.com/nostosx/servex and github.com/nostosx/kitx
```

There are no integration tests.

## License

Licensed under the [MIT License](../LICENSE).

Logger API in the 2026-10-29 release: pass a core logger.Logger, wrapping an explicitly
selected slog or Zap backend. Nil disables logging; backend adapters live in
logger/slog and logger/zap. Historical published versions accept *slog.Logger.

## Release

Version v1.4.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/middleware@v1.4.0
```
