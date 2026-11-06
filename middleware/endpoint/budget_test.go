package endpoint_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
)

func TestBudgetEarlierCallerDeadlineIsPreserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		fn := endpoint.Budget(time.Second)(func(ctx context.Context, _ any) (any, error) { <-ctx.Done(); return nil, ctx.Err() })
		if _, err := fn(ctx, nil); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 50*time.Millisecond {
			t.Fatalf("deadline=%v duration=%v", err, time.Since(start))
		}
	})
}
