package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex/errorx"
)

func Example() {
	failing := func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.Unavailable, "DOWN", "backend down")
	}
	ep := circuitbreaker.New(
		circuitbreaker.WithMinRequests(5),
		circuitbreaker.WithRandom(func() float64 { return 0 }), // deterministic for the example
	)(failing)

	for range 5 {
		_, _ = ep(context.Background(), nil)
	}
	_, err := ep(context.Background(), nil)
	fmt.Println(errors.Is(err, circuitbreaker.ErrOpen), errorx.IsRetryable(err))
	// Output: true false
}
