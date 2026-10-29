// Package playground adapts go-playground/validator to servex middleware.
package playground

import (
	"context"
	"errors"

	"github.com/go-playground/validator/v10"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

const (
	// ReasonValidationFailed identifies a request that violates validation rules.
	ReasonValidationFailed = "VALIDATION_FAILED"
	// ReasonValidationError identifies invalid validation input or configuration.
	ReasonValidationError = "VALIDATION_ERROR"
)

// New validates struct requests with v before invoking the next endpoint.
// Configure v before calling New and do not modify it while requests run.
// Nil v panics. Invalid request shapes are programming errors (Internal);
// field violations are InvalidArgument. Errors retain their original cause
// while public messages contain no request values. Synchronous validation
// cannot be interrupted; cancellation is checked before and after it.
func New(v *validator.Validate) endpoint.Middleware {
	if v == nil {
		panic("playground: nil validator")
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			err := v.StructCtx(ctx, req)
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			if err == nil {
				return next(ctx, req)
			}
			if _, ok := errors.AsType[validator.ValidationErrors](err); ok {
				return nil, errorx.New(errorx.InvalidArgument, ReasonValidationFailed, "request validation failed").WithCause(err)
			}
			return nil, errorx.New(errorx.Internal, ReasonValidationError, "request validation could not be evaluated").WithCause(err)
		}
	}
}
