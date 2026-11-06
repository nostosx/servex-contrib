# observability/metrics/noop

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

Provides an explicit no-op implementation of the core metrics contracts.
Requires the published core contracts from servex v1.7.0.

```go
import metricsnoop "github.com/nostosx/servex-contrib/observability/metrics/noop"

meter := metricsnoop.New()
meter.Counter("requests_total", "Requests", "method").With("GET").Add(ctx, 1)
```

Counters, gauges and histograms discard observations. Label binding always
returns a usable metric and tolerates mismatched labels. All methods are safe
for concurrent use and allocate no memory. Tests preserve these properties.
The core contains only metric contracts and common data; it does not select this
backend implicitly.
