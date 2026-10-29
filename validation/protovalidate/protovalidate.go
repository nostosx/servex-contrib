// Package protovalidate adapts Buf Protovalidate to servex middleware.
package protovalidate

import (
	"context"
	"errors"
	"reflect"

	bufvalidate "buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

const (
	// ReasonValidationFailed identifies a message that violates its schema rules.
	ReasonValidationFailed = "VALIDATION_FAILED"
	// ReasonValidationError identifies invalid validation input or configuration.
	ReasonValidationError = "VALIDATION_ERROR"
)

// New validates protobuf messages with v before invoking the next endpoint.
// A nil validator panics. Requests must be non-nil proto.Message values;
// wrong request shapes and schema/evaluation failures are Internal errors.
// Constraint violations are InvalidArgument. Original errors remain available
// through errors.As; public messages contain no request or CEL details.
// Cancellation is checked before and after synchronous validation, which
// cannot be interrupted by this adapter. The caller owns validator setup.
func New(v bufvalidate.Validator) endpoint.Middleware {
	if isNil(v) {
		panic("protovalidate: nil validator")
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			msg, ok := req.(proto.Message)
			if !ok || isNil(msg) || !msg.ProtoReflect().IsValid() {
				return nil, errorx.New(errorx.Internal, ReasonValidationError, "request must be a non-nil protobuf message")
			}
			err := v.Validate(msg)
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			if err == nil {
				return next(ctx, req)
			}
			if _, ok := errors.AsType[*bufvalidate.ValidationError](err); ok {
				return nil, errorx.New(errorx.InvalidArgument, ReasonValidationFailed, "request validation failed").WithCause(err)
			}
			return nil, errorx.New(errorx.Internal, ReasonValidationError, "request validation could not be evaluated").WithCause(err)
		}
	}
}

func isNil(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
