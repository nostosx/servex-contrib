# observability/metrics/otel

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

`github.com/nostosx/servex-contrib/observability/metrics/otel` implements the servex
`metrics.Meter` on an [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go)
`metric.Meter`.

- Counters map to `Float64Counter`, histograms to `Float64Histogram` with
  explicit bucket boundaries, gauges to `Float64Gauge`. Gauge values are
  tracked per label set by the adapter, so both `Set` and `Add` work.
- Label values become string attributes keyed by the declared label names.
- Recording methods pass the operation's context to the instrument, so the
  SDK can attach exemplars from a sampled span in that context, for example
  one started by [`tracing/otel`](../../tracing/otel).
- Invalid definitions and bindings return no-op metrics. Updates with incomplete
  labels or negative or NaN counter deltas are dropped; later valid updates
  still work. Problems are logged, with dropped updates and invalid bindings
  reported at most once per metric family.

## Usage

The complete programs below use the current source API. Use matching peer
sources during development; install real module versions after publication.

```go
package main

import (
	"context"

	"go.opentelemetry.io/otel"

	otelmetrics "github.com/nostosx/servex-contrib/observability/metrics/otel"
	"github.com/nostosx/servex/observability/metrics"
)

func main() {
	// Uses the meter provider installed by your OpenTelemetry setup, for
	// example an SDK MeterProvider with an OTLP exporter.
	meter := otelmetrics.New(otel.GetMeterProvider().Meter("shop"))

	// Pass the context of the measured operation; a sampled span in it
	// becomes the exemplar of the measurement.
	ctx := context.Background()
	latency := meter.Histogram("checkout_duration_seconds", "Checkout latency.", metrics.DefaultBuckets, "method")
	latency.With("card").Observe(ctx, 0.042)

	inflight := meter.Gauge("checkout_inflight", "Checkouts in progress.")
	inflight.Add(ctx, 1)
	inflight.Add(ctx, -1)
}
```

Pass the Meter to a scope-specific metrics factory under
[`middleware/endpoint`](../../../middleware/endpoint),
[`middleware/application`](../../../middleware/application) or
[`middleware/client`](../../../middleware/client), or to the contrib SQL/GORM
instrumentation. A nil OpenTelemetry meter yields instruments that record nothing.

## Options

| Option | Default | Description |
|---|---|---|
| `WithLogger(l)` | `nil` (disabled) | Logger for misuse reports. |

## Testing

Unit tests use the OpenTelemetry SDK with a manual reader; there are no
integration tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../../LICENSE).

Logger API in the 2026-10-29 release: pass a core logger.Logger, wrapping an explicitly
selected slog or Zap backend. Nil disables logging; backend adapters live in
observability/logger/slog and observability/logger/zap. Historical published versions accept *slog.Logger.
