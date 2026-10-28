package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex/errorx"
)

type contextLimiterFunc func(context.Context, string) (bool, time.Duration, error)

func (f contextLimiterFunc) AllowContext(ctx context.Context, key string) (bool, time.Duration, error) {
	return f(ctx, key)
}

func TestContextLimiter(t *testing.T) {
	t.Parallel()
	backendErr := errors.New("backend failed")
	for _, tc := range []struct {
		name     string
		allowed  bool
		err      error
		failOpen bool
		code     errorx.Code
		called   bool
	}{
		{"allow", true, nil, false, errorx.OK, true},
		{"reject", false, nil, false, errorx.ResourceExhausted, false},
		{"backend", false, backendErr, false, errorx.Unavailable, false},
		{"fail open", false, backendErr, true, errorx.OK, true},
		{"fail open still rejects quota", false, nil, true, errorx.ResourceExhausted, false},
		{"fail open on backend cancellation", false, context.Canceled, true, errorx.OK, true},
		{"fail open on backend timeout", false, context.DeadlineExceeded, true, errorx.OK, true},
		{"backend timeout fails closed", false, context.DeadlineExceeded, false, errorx.Unavailable, false},
		{"typed backend error", false, errorx.New(errorx.FailedPrecondition, "CLOSED", "closed"), false, errorx.FailedPrecondition, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			l := contextLimiterFunc(func(ctx context.Context, key string) (bool, time.Duration, error) {
				if key != "alice" || ctx != t.Context() {
					t.Fatal("context or key not propagated")
				}
				return tc.allowed, 1501 * time.Microsecond, tc.err
			})
			_, err := ratelimit.NewContext(l, ratelimit.WithFailOpen(tc.failOpen), ratelimit.WithKey(func(context.Context, any) string { return "alice" }))(func(context.Context, any) (any, error) {
				called = true
				return nil, nil
			})(t.Context(), nil)
			if errorx.CodeOf(err) != tc.code || called != tc.called {
				t.Fatalf("code=%v called=%v error=%v", errorx.CodeOf(err), called, err)
			}
			if tc.code == errorx.ResourceExhausted && errorx.From(err).Metadata[ratelimit.RetryAfterMetadataKey] != "2" {
				t.Fatalf("retry hint: %v", err)
			}
			if tc.err != nil && err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("backend error identity lost: %v", err)
			}
		})
	}
}

func TestContextLimiterCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	l := contextLimiterFunc(func(context.Context, string) (bool, time.Duration, error) {
		t.Fatal("consulted limiter after cancellation")
		return true, 0, nil
	})
	if _, err := ratelimit.NewContext(l)(ok)(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestContextLimiterCancellationDuringCheck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l := contextLimiterFunc(func(context.Context, string) (bool, time.Duration, error) {
		cancel()
		return false, 0, context.Canceled
	})
	next := func(context.Context, any) (any, error) {
		t.Fatal("caller cancellation ignored by fail-open")
		return nil, nil
	}
	if _, err := ratelimit.NewContext(l, ratelimit.WithFailOpen(true))(next)(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
}
