package ratelimit_test

import (
	"context"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex/errorx"
)

func Example() {
	// One call per second per tenant, with bursts of up to 2 calls.
	limiter := ratelimit.TokenBucket(1, 2)
	mw := ratelimit.New(limiter, ratelimit.WithKey(func(_ context.Context, req any) string {
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
