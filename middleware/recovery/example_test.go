package recovery_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/nostosx/servex-contrib/middleware/recovery"
	"github.com/nostosx/servex/errorx"
)

func Example() {
	logger := testLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ep := recovery.New(recovery.WithLogger(logger))(func(context.Context, any) (any, error) {
		panic("unexpected state")
	})

	_, err := ep(context.Background(), nil)
	e := errorx.From(err)
	fmt.Println(e.Code, e.Reason, e.Retryable())
	// Output: Internal PANIC false
}
