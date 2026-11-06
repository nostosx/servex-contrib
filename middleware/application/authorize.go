// Authorization middleware applies application-owned authorization policies to typed
// calls. Authentication and principal extraction remain explicit dependencies.
package application

import (
	"context"
	"errors"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
)

// ReasonDenied identifies an unclassified policy rejection.
const ReasonDenied = "AUTHORIZATION_DENIED"

// Authorization authorizes each call using its assembly metadata, caller context and typed
// request. check can read an authenticated principal from context; it must not
// trust a request's claimed identity. Nil check panics at configuration time.
// Classified errors pass through; other failures become PermissionDenied with
// their original cause and a fixed public message. A rejection never calls next.
func Authorization[Request, Result any](check func(context.Context, application.Metadata, Request) error) application.Middleware[Request, Result] {
	if check == nil {
		panic("authorize: nil policy")
	}
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			if err := check(ctx, meta, request); err != nil {
				var zero Result
				if _, ok := errors.AsType[*errorx.Error](err); ok {
					return zero, err
				}
				return zero, errorx.New(errorx.PermissionDenied, ReasonDenied, "permission denied").WithCause(err)
			}
			return next.Handle(ctx, request)
		})
	}
}
