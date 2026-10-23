# metrics/prometheus

`github.com/nostosx/servex-contrib/metrics/prometheus` implements the servex
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

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/metrics/prometheus@v1.0.0
```

The example version corresponds to the tag `metrics/prometheus/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nostosx/servex-contrib/metrics/prometheus"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/transport/httpx"
)

func run() error {
	reg := prom.NewRegistry()
	meter := prometheus.New(reg, prometheus.WithNamespace("shop"))

	// Pass meter to anything that takes a metrics.Meter, such as
	// servex-contrib/middleware/metrics or storage/database.WithMeter.
	orders := meter.Counter("orders_total", "Orders placed.", "status")
	orders.With("ok").Add(1)

	srv := httpx.NewServer(httpx.Address(":8000"))
	srv.HandleHTTP("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	return app.New(app.WithHooks(srv)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

A nil registerer uses `prometheus.DefaultRegisterer`.

## Options

| Option | Default | Description |
|---|---|---|
| `WithNamespace(ns)` | none | Prefixes every metric name with `ns_`. |
| `WithLogger(l)` | `slog.Default()` | Logger for misuse reports. |

## Testing

Unit tests use the Prometheus `testutil` package; there are no integration
tests.

```sh
GOWORK=off go test -race -count=1 ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
