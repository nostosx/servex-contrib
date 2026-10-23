package validate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/validate"
	"github.com/nostosx/servex/errorx"
)

type request struct{ err error }

func (r *request) Validate() error { return r.err }

func echo(_ context.Context, req any) (any, error) { return req, nil }

func TestValid(t *testing.T) {
	req := &request{}
	resp, err := validate.New()(echo)(t.Context(), req)
	if err != nil || resp != req {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}

func TestNotValidator(t *testing.T) {
	resp, err := validate.New()(echo)(t.Context(), "plain")
	if err != nil || resp != "plain" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
	if _, err := validate.New()(echo)(t.Context(), nil); err != nil {
		t.Fatalf("nil request: %v", err)
	}
}

func TestInvalid(t *testing.T) {
	sentinel := errors.New("name is required")
	called := false
	next := func(context.Context, any) (any, error) { called = true; return nil, nil }
	_, err := validate.New()(next)(t.Context(), &request{err: sentinel})
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.InvalidArgument || e.Reason != validate.ReasonValidationFailed {
		t.Fatalf("err = %v", err)
	}
	if e.Message != "name is required" || !errors.Is(err, sentinel) {
		t.Fatalf("message %q, cause kept %v", e.Message, errors.Is(err, sentinel))
	}
	if errorx.IsRetryable(err) {
		t.Fatal("validation error must not be retryable")
	}
	if called {
		t.Fatal("next endpoint called")
	}
}

func TestErrorxUnchanged(t *testing.T) {
	custom := errorx.New(errorx.OutOfRange, "PAGE_TOO_LARGE", "page too large")
	_, err := validate.New()(echo)(t.Context(), &request{err: custom})
	if err != custom {
		t.Fatalf("err = %v, want unchanged", err)
	}
	wrapped := errors.Join(custom)
	if _, err := validate.New()(echo)(t.Context(), &request{err: wrapped}); err != wrapped {
		t.Fatalf("err = %v, want wrapped errorx unchanged", err)
	}
}
