package endpoint_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
)

func ExampleRecovery() {
	logger := recoveryTestLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ep := endpoint.Recovery(endpoint.WithRecoveryLogger(logger))(func(context.Context, any) (any, error) {
		panic("unexpected state")
	})

	_, err := ep(context.Background(), nil)
	e := errorx.From(err)
	fmt.Println(e.Code, e.Reason, e.Retryable())
	// Output: Internal PANIC false
}
