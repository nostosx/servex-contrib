package tracing_test

import (
	"context"
	"fmt"

	mwtracing "github.com/nostosx/servex-contrib/middleware/tracing"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

func Example() {
	// Use a real Tracer and Propagator, such as the ones from
	// servex-contrib/tracing/otel, in production.
	tracer, propagator := tracing.Noop(), tracing.NoopPropagator()
	ep := mwtracing.New(tracer, propagator)(func(ctx context.Context, _ any) (any, error) {
		// The handler sees the server span in its context.
		_ = tracing.SpanFromContext(ctx)
		return "ok", nil
	})

	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /v1/users/{id}",
		Header: metadata.Pairs("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
	})
	resp, err := ep(ctx, nil)
	fmt.Println(resp, err)
	// Output: ok <nil>
}
