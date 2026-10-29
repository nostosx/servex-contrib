# metrics/noop

Provides an explicit no-op implementation of the core metrics contracts.
Requires the published core contracts from servex v1.4.0.

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

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/metrics/noop@v1.0.0
```
