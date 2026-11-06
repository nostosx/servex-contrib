package endpoint_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
)

type request struct{ err error }

func (r *request) Validate() error { return r.err }

func echo(_ context.Context, req any) (any, error) { return req, nil }

func TestValidationValid(t *testing.T) {
	req := &request{}
	resp, err := endpoint.Validation()(echo)(t.Context(), req)
	if err != nil || resp != req {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}

func TestValidationNotValidator(t *testing.T) {
	resp, err := endpoint.Validation()(echo)(t.Context(), "plain")
	if err != nil || resp != "plain" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
	if _, err := endpoint.Validation()(echo)(t.Context(), nil); err != nil {
		t.Fatalf("nil request: %v", err)
	}
}

func TestValidationInvalid(t *testing.T) {
	sentinel := errors.New("name is required")
	called := false
	next := func(context.Context, any) (any, error) { called = true; return nil, nil }
	_, err := endpoint.Validation()(next)(t.Context(), &request{err: sentinel})
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.InvalidArgument || e.Reason != endpoint.ReasonValidationFailed {
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

func TestValidationErrorxUnchanged(t *testing.T) {
	custom := errorx.New(errorx.OutOfRange, "PAGE_TOO_LARGE", "page too large")
	_, err := endpoint.Validation()(echo)(t.Context(), &request{err: custom})
	if err != custom {
		t.Fatalf("err = %v, want unchanged", err)
	}
	wrapped := errors.Join(custom)
	if _, err := endpoint.Validation()(echo)(t.Context(), &request{err: wrapped}); err != wrapped {
		t.Fatalf("err = %v, want wrapped errorx unchanged", err)
	}
}
