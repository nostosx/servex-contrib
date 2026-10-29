package logging_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/nostosx/servex-contrib/middleware/logging"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func Example() {
	// Drop the time and duration so the output is stable.
	logger := testLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == logging.KeyDuration {
				return slog.Attr{}
			}
			return a
		},
	})))
	ep := logging.New(logger)(func(_ context.Context, req any) (any, error) {
		if req == nil {
			return nil, errorx.New(errorx.NotFound, "USER_NOT_FOUND", "user not found")
		}
		return req, nil
	})

	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /v1/users/{id}",
	})
	_, _ = ep(ctx, "alice")
	_, _ = ep(ctx, nil)
	// Output:
	// level=INFO msg=call kind=server protocol=http operation="GET /v1/users/{id}" code=OK
	// level=WARN msg=call kind=server protocol=http operation="GET /v1/users/{id}" code=NotFound reason=USER_NOT_FOUND error="code=NotFound reason=USER_NOT_FOUND message=user not found"
}
