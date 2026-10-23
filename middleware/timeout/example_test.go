package timeout_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nostosx/servex-contrib/middleware/timeout"
	"github.com/nostosx/servex/errorx"
)

func Example() {
	ep := timeout.New(10 * time.Millisecond)(func(ctx context.Context, _ any) (any, error) {
		<-ctx.Done() // a slow backend that honours cancellation
		return nil, ctx.Err()
	})

	_, err := ep(context.Background(), nil)
	e := errorx.From(err)
	fmt.Println(e.Code, e.Reason, e.Retryable())
	// Output: DeadlineExceeded TIMEOUT true
}
