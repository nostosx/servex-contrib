# observability/metrics/prometheus

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

`github.com/nostosx/servex-contrib/observability/metrics/prometheus` implements the servex
`metrics.Meter` on the [Prometheus Go client](https://github.com/prometheus/client_golang).
Counters, gauges and histograms map to `CounterVec`, `GaugeVec` and
`HistogramVec` collectors registered on a `prometheus.Registerer` you supply.

- Each metric name is registered once per Meter; asking again with the same
  name, kind and labels returns the same collector, and an identical collector
  already in the registry is reused.
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
	"log"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nostosx/servex-contrib/observability/metrics/prometheus"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex-contrib/transport/httpx"
)

func run() error {
	reg := prom.NewRegistry()
	meter := prometheus.New(reg, prometheus.WithNamespace("shop"))

	// Pass meter to anything that takes a metrics.Meter, such as
	// middleware/endpoint.Metrics, middleware/application.Metrics or middleware/client.Metrics.
	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	// Recording methods take the context of the measured operation.
	orders := meter.Counter("orders_total", "Orders placed.", "status")
	orders.With("ok").Add(ctx, 1)

	srv := httpx.NewServer(httpx.WithAddress(":8000"))
	srv.HandleHTTP("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return app.New(app.WithHooks(srv)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

A nil registerer uses `prometheus.DefaultRegisterer`.
Recording methods accept the context required by servex v1.2.0 metrics but do
not read it; this adapter records no exemplars.

## Options

| Option | Default | Description |
|---|---|---|
| `WithNamespace(ns)` | none | Prefixes every metric name with `ns_`. |
| `WithLogger(l)` | `nil` (disabled) | Logger for misuse reports. |

## Testing

Unit tests use the Prometheus `testutil` package; there are no integration
tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../../LICENSE).

Logger API in the 2026-10-29 release: pass a core logger.Logger, wrapping an explicitly
selected slog or Zap backend. Nil disables logging; backend adapters live in
observability/logger/slog and observability/logger/zap. Historical published versions accept *slog.Logger.
