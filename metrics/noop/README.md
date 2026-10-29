# metrics/noop

Provides an explicit no-op implementation of the core metrics contracts.
This adapter is unreleased and requires the matching core source revision
until a coordinated release.

```go
import metricsnoop "github.com/nostosx/servex-contrib/metrics/noop"

meter := metricsnoop.New()
meter.Counter("requests_total", "Requests", "method").With("GET").Add(ctx, 1)
```

Counters, gauges and histograms discard observations. Label binding always
returns a usable metric and tolerates mismatched labels. All methods are safe
for concurrent use and allocate no memory. Tests preserve these properties.
The core contains only metric contracts and common data; it does not select this
backend implicitly.
