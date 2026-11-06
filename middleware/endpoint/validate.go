package endpoint

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/validation"

	"github.com/nostosx/servex/endpoint"
)

// ReasonValidationFailed is the errorx reason of errors produced from
// validation failures.
const ReasonValidationFailed = validation.ReasonValidationFailed

// Validator is implemented by requests that can validate themselves.
type Validator = validation.Validator

// Validation returns middleware that calls Validate on requests implementing
// Validator before calling the next endpoint; other requests pass through.
//
// When Validate fails with an error that already carries an *errorx.Error,
// that error is returned unchanged. Any other error yields
// errorx.InvalidArgument with reason ReasonValidationFailed, the validation
// error's text as its Message and the original error as its cause. The next
// endpoint is not called on failure.
func Validation() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if v, ok := req.(Validator); ok {
				if err := v.Validate(); err != nil {
					return nil, validation.Error(err)
				}
			}
			return next(ctx, req)
		}
	}
}
