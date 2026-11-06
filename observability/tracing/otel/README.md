# observability/tracing/otel

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../../CONTRIBUTING.md#checks).

`github.com/nostosx/servex-contrib/observability/tracing/otel` adapts
[OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go) tracing to
the servex tracing API.

- `NewTracer(t)` wraps an `oteltrace.Tracer` as a `tracing.Tracer`.
- `NewPropagator(p)` wraps a `propagation.TextMapPropagator` as a
  `tracing.Propagator` that works on any `tracing.Carrier`, such as
  `metadata.MD`. A nil propagator uses `propagation.TraceContext{}`.
- Spans interoperate in both directions: a context returned by the Tracer
  carries the span for `oteltrace.SpanFromContext` (so OpenTelemetry
  instrumentation sees it) and for `tracing.SpanFromContext`; spans started
  directly through OpenTelemetry become parents of spans started through the
  Tracer.

The adapter records errors as safe code/reason summaries with
`servex.code`, `servex.reason`, `servex.retryable`, `servex.remote` and `error.type`.
Original message text, metadata and causes are excluded from exception events
and status descriptions. This source targets published servex v1.7.0; older
adapter releases retain their release-time behavior.

## Usage

The complete programs below use the current source API. Use matching peer
sources during development; install real module versions after publication.

```go
package main

import (
	"context"
	"log"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	oteltracing "github.com/nostosx/servex-contrib/observability/tracing/otel"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

func main() {
	// Add an exporter with sdktrace.WithBatcher in a real setup.
	tp := sdktrace.NewTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Print(err)
		}
	}()

	tracer := oteltracing.NewTracer(tp.Tracer("user-service"))
	propagator := oteltracing.NewPropagator(propagation.TraceContext{})

	ctx, span := tracer.Start(context.Background(), "load-user", tracing.SpanKindInternal)
	defer span.End()
	span.SetAttribute("user.id", 42)

	// metadata.MD is a tracing.Carrier: Inject writes the W3C traceparent.
	md := metadata.MD{}
	propagator.Inject(ctx, md)
	log.Println(md.Get("traceparent"))
}
```

Pass this backend to the tracing factory for the execution boundary:
[`middleware/endpoint`](../../../middleware/endpoint) for inbound
calls, [`middleware/client`](../../../middleware/client) for outbound
calls, or [`middleware/application`](../../../middleware/application)
for typed business handlers. Endpoint and client Tracing accept a propagator;
applicationmw.Tracing[Request, Result] creates an internal span without transport headers.

## Testing

Unit tests use the OpenTelemetry SDK with an in-memory exporter; there are no
integration tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../../LICENSE).
