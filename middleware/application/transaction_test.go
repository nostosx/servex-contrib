package application_test

import (
	"context"
	"errors"
	"testing"

	applicationmiddleware "github.com/nostosx/servex-contrib/middleware/application"

	"github.com/nostosx/servex/application"
)

type runFunc func(context.Context, func(context.Context) error) error

func (r runFunc) Within(ctx context.Context, fn func(context.Context) error) error { return r(ctx, fn) }

func TestResultPublishedOnlyAfterCommit(t *testing.T) {
	t.Parallel()
	failed := errors.New("transaction failure")
	for _, phase := range []string{"begin", "business", "commit", "success"} {
		t.Run(phase, func(t *testing.T) {
			type key struct{}
			calls, commits := 0, 0
			runner := runFunc(func(ctx context.Context, fn func(context.Context) error) error {
				if phase == "begin" {
					return failed
				}
				if err := fn(context.WithValue(ctx, key{}, true)); err != nil {
					return err
				}
				commits++
				if phase == "commit" {
					return failed
				}
				return nil
			})
			handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(ctx context.Context, request int) (int, error) {
				calls++
				if ctx.Value(key{}) != true {
					t.Fatal("transaction context was lost")
				}
				if phase == "business" {
					return 42, failed
				}
				return request, nil
			}, applicationmiddleware.Transaction[int, int](runner))
			result, err := handler(t.Context(), 7)
			if phase == "success" {
				if result != 7 || err != nil || calls != 1 || commits != 1 {
					t.Fatalf("successful transaction: %d %v %d %d", result, err, calls, commits)
				}
			} else if result != 0 || err != failed || calls > 1 || (phase == "begin" && calls != 0) || (phase == "business" && commits != 0) {
				t.Fatalf("failed transaction: %d %v %d %d", result, err, calls, commits)
			}
		})
	}
}

func TestPanicPropagatesThroughRunnerCleanup(t *testing.T) {
	t.Parallel()
	marker := &struct{}{}
	cleaned := false
	runner := runFunc(func(ctx context.Context, fn func(context.Context) error) error {
		defer func() { cleaned = true }()
		return fn(ctx)
	})
	handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(context.Context, int) (int, error) { panic(marker) }, applicationmiddleware.Transaction[int, int](runner))
	defer func() {
		if got := recover(); got != marker || !cleaned {
			t.Fatalf("panic=%v cleaned=%v", got, cleaned)
		}
	}()
	_, _ = handler(t.Context(), 1)
}
