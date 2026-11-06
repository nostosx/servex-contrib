package endpoint_test

import (
	"context"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
)

func ExampleRateLimit() {
	// One call per second per tenant, with bursts of up to 2 calls.
	limiter := endpoint.TokenBucket(1, 2)
	mw := endpoint.RateLimit(limiter, endpoint.WithRateLimitKey(func(_ context.Context, req any) string {
		return req.(string) // for example the tenant of the request
	}))
	ep := mw(func(context.Context, any) (any, error) { return "ok", nil })

	for range 3 {
		_, err := ep(context.Background(), "tenant-a")
		fmt.Println(errorx.CodeOf(err))
	}
	_, err := ep(context.Background(), "tenant-b")
	fmt.Println(errorx.CodeOf(err))
	// Output:
	// OK
	// OK
	// ResourceExhausted
	// OK
}
