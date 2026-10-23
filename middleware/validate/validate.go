package validate

import (
	"context"
	"errors"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// ReasonValidationFailed is the errorx reason of errors produced from
// validation failures.
const ReasonValidationFailed = "VALIDATION_FAILED"

// Validator is implemented by requests that can validate themselves.
type Validator interface {
	Validate() error
}

// New returns middleware that calls Validate on requests implementing
// Validator before calling the next endpoint; other requests pass through.
//
// When Validate fails with an error that already carries an *errorx.Error,
// that error is returned unchanged. Any other error yields
// errorx.InvalidArgument with reason ReasonValidationFailed, the validation
// error's text as its Message and the original error as its cause. The next
// endpoint is not called on failure.
func New() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if v, ok := req.(Validator); ok {
				if err := v.Validate(); err != nil {
					if _, ok := errors.AsType[*errorx.Error](err); ok {
						return nil, err
					}
					return nil, errorx.New(errorx.InvalidArgument, ReasonValidationFailed, err.Error()).WithCause(err)
				}
			}
			return next(ctx, req)
		}
	}
}
