package application_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	applicationmiddleware "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex/application"
)

func TestEarlierCallerDeadlineAndResultIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		result := new(int)
		handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(ctx context.Context, _ int) (*int, error) {
			<-ctx.Done()
			return result, context.Cause(ctx)
		}, applicationmiddleware.Budget[int, *int](time.Second))
		start := time.Now()
		got, err := handler(ctx, 3)
		if got != result || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 50*time.Millisecond {
			t.Fatalf("result=%p error=%v duration=%v", got, err, time.Since(start))
		}
	})
}

func TestDisabledBudgetPreservesContext(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Second} {
		t.Run(duration.String(), func(t *testing.T) {
			ctx := t.Context()
			handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(got context.Context, request int) (int, error) {
				if got != ctx {
					t.Fatal("disabled budget changed caller context")
				}
				return request, nil
			}, applicationmiddleware.Budget[int, int](duration))
			if result, err := handler(ctx, 3); result != 3 || err != nil {
				t.Fatalf("result=%d error=%v", result, err)
			}
		})
	}
}
