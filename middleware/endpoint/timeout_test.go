package endpoint

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestTimeoutRejectsLateSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		call := Timeout(time.Second)(func(ctx context.Context, _ any) (any, error) {
			<-ctx.Done()
			return "late response", nil
		})
		response, err := call(t.Context(), nil)
		if response != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("deadline expired but call reported success", response, err)
		}
	})
}

func TestTimeoutKeepsEarlierDeadlineAndDisabledContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for _, duration := range []time.Duration{0, -time.Second, time.Minute} {
		call := Timeout(duration)(func(actual context.Context, _ any) (any, error) {
			got, ok := actual.Deadline()
			if !ok || got.After(deadline) {
				t.Fatal("caller deadline extended", got)
			}
			return "response", nil
		})
		if response, err := call(ctx, nil); err != nil || response != "response" {
			t.Fatal(response, err)
		}
	}
}
