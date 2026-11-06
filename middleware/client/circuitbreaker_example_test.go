package client_test

import (
	"context"
	"errors"
	"fmt"

	clientmiddleware "github.com/nostosx/servex-contrib/middleware/client"
	"github.com/nostosx/servex/errorx"
)

func ExampleCircuitBreaker() {
	failing := func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.Unavailable, "DOWN", "backend down")
	}
	ep := clientmiddleware.CircuitBreaker(
		clientmiddleware.WithCircuitBreakerMinRequests(5),
		clientmiddleware.WithCircuitBreakerRandom(func() float64 { return 0 }), // deterministic for the example
	)(failing)

	for range 5 {
		_, _ = ep(context.Background(), nil)
	}
	_, err := ep(context.Background(), nil)
	fmt.Println(errors.Is(err, clientmiddleware.ErrOpen), errorx.IsRetryable(err))
	// Output: true false
}
