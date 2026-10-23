# tracing/otel

`github.com/nostosx/servex-contrib/tracing/otel` adapts
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

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/tracing/otel@v1.0.0
```

The example version corresponds to the tag `tracing/otel/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	oteltracing "github.com/nostosx/servex-contrib/tracing/otel"
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

The usual consumer is the tracing middleware:
`tracing.New(tracer, propagator)` from
[`middleware/tracing`](../../middleware), which continues inbound traces on
servers and injects the span into outbound headers on clients.

## Testing

Unit tests use the OpenTelemetry SDK with an in-memory exporter; there are no
integration tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
