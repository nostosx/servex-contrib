# metrics/otel

`github.com/nostosx/servex-contrib/metrics/otel` implements the servex
`metrics.Meter` on an [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go)
`metric.Meter`.

- Counters map to `Float64Counter`, histograms to `Float64Histogram` with
  explicit bucket boundaries, gauges to `Float64Gauge`. Gauge values are
  tracked per label set by the adapter, so both `Set` and `Add` work.
- Label values become string attributes keyed by the declared label names.
- Invalid definitions and bindings return no-op metrics. Updates with incomplete
  labels or negative or NaN counter deltas are dropped; later valid updates
  still work. Problems are logged, with dropped updates and invalid bindings
  reported at most once per metric family.

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/metrics/otel@v1.0.0
```

The example version corresponds to the tag `metrics/otel/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"go.opentelemetry.io/otel"

	otelmetrics "github.com/nostosx/servex-contrib/metrics/otel"
	"github.com/nostosx/servex/observability/metrics"
)

func main() {
	// Uses the meter provider installed by your OpenTelemetry setup, for
	// example an SDK MeterProvider with an OTLP exporter.
	meter := otelmetrics.New(otel.GetMeterProvider().Meter("shop"))

	latency := meter.Histogram("checkout_duration_seconds", "Checkout latency.", metrics.DefaultBuckets, "method")
	latency.With("card").Observe(0.042)

	inflight := meter.Gauge("checkout_inflight", "Checkouts in progress.")
	inflight.Add(1)
	inflight.Add(-1)
}
```

Pass the Meter to anything that takes a `metrics.Meter`, such as
[`middleware/metrics`](../../middleware) or the core
`storage/database.WithMeter`. A nil OpenTelemetry meter yields instruments that
record nothing.

## Options

| Option | Default | Description |
|---|---|---|
| `WithLogger(l)` | `slog.Default()` | Logger for misuse reports. |

## Testing

Unit tests use the OpenTelemetry SDK with a manual reader; there are no
integration tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
