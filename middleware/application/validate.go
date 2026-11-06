// Validation middleware validates typed application requests before business execution.
package application

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/validation"
	"github.com/nostosx/servex/application"
)

const ReasonValidationFailed = validation.ReasonValidationFailed

// Validator is implemented by requests that validate themselves.
type Validator = validation.Validator

// Validation validates typed business requests before execution. A nil check
// uses the request's Validator method when present; an explicit check can adapt
// an injected validation engine. Validation failures return the zero result.
// Error classification matches endpoint validation. Construct once during assembly.
func Validation[Request, Result any](check func(context.Context, Request) error) application.Middleware[Request, Result] {
	return func(_ application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			var err error
			if check != nil {
				err = check(ctx, request)
			} else if validator, ok := any(request).(validation.Validator); ok {
				err = validator.Validate()
			}
			if err != nil {
				var zero Result
				return zero, validation.Error(err)
			}
			return next.Handle(ctx, request)
		})
	}
}
