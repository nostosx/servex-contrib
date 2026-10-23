package retry_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex-contrib/middleware/retry"
	"github.com/nostosx/servex-contrib/middleware/timeout"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

func Example() {
	attempts := 0
	flaky := func(_ context.Context, req any) (any, error) {
		attempts++
		if attempts < 3 {
			return nil, errorx.New(errorx.Unavailable, "DOWN", "temporarily down")
		}
		return req, nil
	}

	// The recommended client order: retry -> circuitbreaker -> timeout.
	ep := endpoint.Chain(
		retry.New(retry.WithBackoff(func(int) time.Duration { return time.Millisecond })),
		circuitbreaker.New(),
		timeout.New(time.Second),
	)(flaky)

	resp, err := ep(context.Background(), "hello")
	fmt.Println(resp, err, attempts)
	// Output: hello <nil> 3
}
