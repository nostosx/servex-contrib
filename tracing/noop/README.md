# tracing/noop

Provides explicit no-op implementations of the core tracing contracts.
This adapter is unreleased and requires the matching core source revision
until a coordinated release.

```go
import tracingnoop "github.com/nostosx/servex-contrib/tracing/noop"

tracer := tracingnoop.New()
propagator := tracingnoop.NewPropagator()
```

The tracer returns a usable Span with empty IDs and discards span updates.
Without a current Span it returns the original context without allocating;
the core's SpanFromContext continues to return nil for that absent span.
When a real parent Span exists, the tracer places its no-op Span in the returned
context so that later instrumentation cannot accidentally modify the parent.
Repeated starts from an existing no-op parent keep the context unchanged.

The propagator does not modify carriers and returns extraction contexts unchanged.
Implementations are safe for concurrent use. The regression suite retains context
isolation, nil context, no-allocation and concurrency checks.
