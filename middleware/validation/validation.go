// Package validation shares request validation classification across boundaries.
package validation

import (
	"errors"

	"github.com/nostosx/servex/errorx"
)

const ReasonValidationFailed = "VALIDATION_FAILED"

type Validator interface{ Validate() error }

func Error(err error) error {
	if _, ok := errors.AsType[*errorx.Error](err); ok {
		return err
	}
	return errorx.New(errorx.InvalidArgument, ReasonValidationFailed, err.Error()).WithCause(err)
}
