package metrics_test

import (
	"context"
	"fmt"

	noopmetrics "github.com/nostosx/servex-contrib/metrics/noop"

	mwmetrics "github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex/endpoint"
)

func Example() {
	// Use a real Meter, such as the one from
	// servex-contrib/metrics/prometheus, in production.
	meter := noopmetrics.New()

	chain := endpoint.Chain(
		mwmetrics.New(meter, mwmetrics.WithBuckets([]float64{.01, .1, 1})),
	)
	ep := chain(func(context.Context, any) (any, error) { return "ok", nil })

	resp, err := ep(context.Background(), nil)
	fmt.Println(resp, err)
	// Output: ok <nil>
}
