package timeout_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/timeout"
	"github.com/nostosx/servex/errorx"
)

// blocking is an endpoint that waits for its context to end and returns the
// context error.
func blocking(ctx context.Context, _ any) (any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestDisabled(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		ep := timeout.New(d)(func(ctx context.Context, _ any) (any, error) {
			if _, ok := ctx.Deadline(); ok {
				t.Errorf("New(%v): context has a deadline", d)
			}
			return "ok", nil
		})
		if resp, err := ep(t.Context(), nil); err != nil || resp != "ok" {
			t.Fatalf("New(%v) = %v, %v; want ok, nil", d, resp, err)
		}
	}
}

func TestDeadlineSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ep := timeout.New(time.Second)(func(ctx context.Context, req any) (any, error) {
			dl, ok := ctx.Deadline()
			if !ok {
				t.Fatal("context has no deadline")
			}
			if got := dl.Sub(start); got != time.Second {
				t.Errorf("deadline in %v, want 1s", got)
			}
			return req, nil
		})
		if resp, err := ep(t.Context(), "req"); err != nil || resp != "req" {
			t.Fatalf("ep = %v, %v; want req, nil", resp, err)
		}
	})
}

func TestExceeded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		_, err := timeout.New(500*time.Millisecond)(blocking)(t.Context(), nil)
		if elapsed := time.Since(start); elapsed != 500*time.Millisecond {
			t.Errorf("elapsed = %v, want 500ms", elapsed)
		}
		e, ok := errors.AsType[*errorx.Error](err)
		if !ok {
			t.Fatalf("err = %v, want *errorx.Error", err)
		}
		if e.Code != errorx.DeadlineExceeded || e.Reason != timeout.ReasonTimeout {
			t.Errorf("err = %v, want DeadlineExceeded/%s", err, timeout.ReasonTimeout)
		}
		if !e.Retryable() {
			t.Error("timeout error should be retryable")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Error("cause context.DeadlineExceeded lost")
		}
	})
}

// TestExceededWrapsAnyError checks that any error returned after the attempt
// deadline is reported as a timeout, keeping the original as cause.
func TestExceededWrapsAnyError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errorx.New(errorx.Unavailable, "CONN", "connection reset")
		_, err := timeout.New(time.Second)(func(ctx context.Context, _ any) (any, error) {
			<-ctx.Done()
			return nil, cause
		})(t.Context(), nil)
		if errorx.CodeOf(err) != errorx.DeadlineExceeded {
			t.Fatalf("CodeOf = %v, want DeadlineExceeded", errorx.CodeOf(err))
		}
		if !errors.Is(err, cause) {
			t.Error("original error not kept as cause")
		}
	})
}

func TestErrorBeforeDeadlineUnchanged(t *testing.T) {
	want := errorx.New(errorx.NotFound, "USER_NOT_FOUND", "no such user")
	_, err := timeout.New(time.Minute)(func(context.Context, any) (any, error) {
		return nil, want
	})(t.Context(), nil)
	if err != want {
		t.Fatalf("err = %v, want %v unchanged", err, want)
	}
}

func TestLateSuccessReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		resp, err := timeout.New(time.Second)(func(ctx context.Context, _ any) (any, error) {
			<-ctx.Done()
			return "late", nil
		})(t.Context(), nil)
		if err != nil || resp != "late" {
			t.Fatalf("ep = %v, %v; want late, nil", resp, err)
		}
	})
}

// TestCallerDeadlineUnchanged checks that an earlier deadline of the caller
// is not reported as an attempt timeout.
func TestCallerDeadlineUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err := timeout.New(time.Second)(blocking)(ctx, nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
		if _, ok := errors.AsType[*errorx.Error](err); ok {
			t.Errorf("err = %v, want the caller's context error unchanged", err)
		}
	})
}

func TestCallerCancelUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(10*time.Millisecond, cancel)
		_, err := timeout.New(time.Second)(blocking)(ctx, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if errorx.CodeOf(err) != errorx.Canceled {
			t.Errorf("CodeOf = %v, want Canceled", errorx.CodeOf(err))
		}
	})
}
