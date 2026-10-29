package playground_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/nostosx/servex-contrib/validation/playground"
	"github.com/nostosx/servex/errorx"
)

type request struct {
	Name  string `validate:"required"`
	Email string `validate:"email"`
}

func TestValidation(t *testing.T) {
	v := validator.New(validator.WithRequiredStructEnabled())
	for _, tt := range []struct {
		name string
		req  any
		code errorx.Code
	}{
		{name: "valid pointer", req: &request{Name: "alice", Email: "alice@example.com"}},
		{name: "valid value", req: request{Name: "alice", Email: "alice@example.com"}},
		{name: "field violations", req: &request{Email: "private-invalid-value"}, code: errorx.InvalidArgument},
		{name: "nil input", code: errorx.Internal},
		{name: "typed nil", req: (*request)(nil), code: errorx.Internal},
		{name: "wrong endpoint type", req: "private-invalid-value", code: errorx.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			businessErr := errors.New("business error")
			ep := playground.New(v)(func(ctx context.Context, req any) (any, error) {
				calls++
				if req != tt.req || ctx != t.Context() {
					t.Fatal("request or context changed")
				}
				return "response", businessErr
			})
			resp, err := ep(t.Context(), tt.req)
			if tt.code == 0 {
				if calls != 1 || resp != "response" || err != businessErr {
					t.Fatalf("next result = %v, %v; calls = %d", resp, err, calls)
				}
				return
			}
			if calls != 0 || resp != nil {
				t.Fatalf("invalid request reached next: %v, %d", resp, calls)
			}
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok || e.Code != tt.code {
				t.Fatalf("error = %v, want %v", err, tt.code)
			}
			if tt.code == errorx.InvalidArgument {
				if e.Reason != playground.ReasonValidationFailed || e.Message != "request validation failed" {
					t.Fatalf("public error = %+v", e)
				}
				if fields, ok := errors.AsType[validator.ValidationErrors](err); !ok || len(fields) != 2 {
					t.Fatalf("validation cause = %v", err)
				}
			} else if _, ok := errors.AsType[*validator.InvalidValidationError](err); !ok || e.Reason != playground.ReasonValidationError {
				t.Fatalf("invalid input cause = %v", err)
			}
		})
	}
}

func TestCancellation(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "during validation"
		if before {
			name = "before validation"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := errors.New("request aborted")
			v := validator.New()
			checks := 0
			if err := v.RegisterValidationCtx("abort", func(got context.Context, _ validator.FieldLevel) bool {
				checks++
				if got != ctx {
					t.Fatal("custom validator lost context")
				}
				cancel(cause)
				return false
			}); err != nil {
				t.Fatal(err)
			}
			if before {
				cancel(cause)
			}
			ep := playground.New(v)(func(context.Context, any) (any, error) {
				t.Fatal("canceled request reached next")
				return nil, nil
			})
			_, err := ep(ctx, struct {
				Name string `validate:"abort"`
			}{Name: "alice"})
			if err != cause || (before && checks != 0) || (!before && checks != 1) {
				t.Fatalf("error = %v, checks = %d", err, checks)
			}
		})
	}
}

func TestConcurrentValidation(t *testing.T) {
	v := validator.New()
	ep := playground.New(v)(func(context.Context, any) (any, error) { return "ok", nil })
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if _, err := ep(t.Context(), request{Name: "alice", Email: "alice@example.com"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestNilValidator(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil validator did not panic")
		}
	}()
	playground.New(nil)
}
