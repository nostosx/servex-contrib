package endpoint

import (
	"context"
	"errors"

	"github.com/nostosx/servex-contrib/middleware/tokenauth"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// Reasons of the errors returned by the middleware.
const (
	// ReasonMissingToken means the call carried no token.
	ReasonMissingToken = tokenauth.ReasonMissingToken
	// ReasonInvalidToken means the token was malformed or rejected by verify.
	ReasonInvalidToken = tokenauth.ReasonInvalidToken
)

// Default header and scheme.
const (
	DefaultTokenHeader = tokenauth.DefaultHeader
	DefaultTokenScheme = tokenauth.DefaultScheme
)

// AuthOption configures inbound token extraction.
type AuthOption = tokenauth.Option

// WithTokenHeader selects the case-insensitive header; empty keeps authorization.
func WithTokenHeader(name string) AuthOption { return tokenauth.WithHeader(name) }

// WithTokenScheme selects the token prefix; empty treats the complete value as a token.
func WithTokenScheme(scheme string) AuthOption { return tokenauth.WithScheme(scheme) }

// principalKey is the context key under which a principal of type P is
// stored; each P has its own key.
type principalKey[P any] struct{}

// NewPrincipalContext returns a copy of ctx carrying the principal p, as Authentication does
// after a successful verification. It is useful for testing handlers that
// call PrincipalFromContext.
func NewPrincipalContext[P any](ctx context.Context, p P) context.Context {
	return context.WithValue(ctx, principalKey[P]{}, p)
}

// PrincipalFromContext returns the principal of type P stored in ctx by Authentication, if any.
// P must be the same type argument as the one used with Authentication.
func PrincipalFromContext[P any](ctx context.Context) (P, bool) {
	if ctx == nil {
		var zero P
		return zero, false
	}
	p, ok := ctx.Value(principalKey[P]{}).(P)
	return p, ok
}

// Authentication returns server middleware that authenticates calls with verify.
//
// The token is read from the configured header of the metadata.Info in the
// context and the scheme prefix, compared case-insensitively, is removed.
// A missing Info, header or token yields errorx.Unauthenticated with reason
// ReasonMissingToken; a value with a different scheme yields reason
// ReasonInvalidToken. verify is then called with the token; on success its
// principal is stored in the context passed to the next endpoint, where
// PrincipalFromContext[P] returns it.
//
// If verify fails with an error that already carries an *errorx.Error, for
// example errorx.PermissionDenied or errorx.Unavailable when a key server is
// down, that error is returned unchanged. Any other error yields
// errorx.Unauthenticated with reason ReasonInvalidToken and the original
// error as its cause.
func Authentication[P any](verify func(ctx context.Context, token string) (P, error), opts ...AuthOption) endpoint.Middleware {
	if verify == nil {
		panic("endpoint: nil token verifier")
	}
	o := tokenauth.NewOptions(opts)
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			token, err := o.Token(ctx)
			if err != nil {
				return nil, err
			}
			p, err := verify(ctx, token)
			if err != nil {
				if _, ok := errors.AsType[*errorx.Error](err); ok {
					return nil, err
				}
				return nil, errorx.New(errorx.Unauthenticated, ReasonInvalidToken, "invalid token").WithCause(err)
			}
			return next(NewPrincipalContext(ctx, p), req)
		}
	}
}
