// Authentication middleware injects application-supplied credentials into outbound calls.
package client

import (
	"context"
	"errors"

	"github.com/nostosx/servex-contrib/middleware/tokenauth"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

const (
	ReasonTokenUnavailable = "TOKEN_UNAVAILABLE"
	DefaultTokenHeader     = tokenauth.DefaultHeader
	DefaultTokenScheme     = tokenauth.DefaultScheme
)

// AuthOption configures outbound credential injection.
type AuthOption = tokenauth.Option

// WithTokenHeader selects the case-insensitive outbound header; empty keeps authorization.
func WithTokenHeader(name string) AuthOption { return tokenauth.WithHeader(name) }

// WithTokenScheme selects the outbound token prefix; empty writes the token alone.
func WithTokenScheme(scheme string) AuthOption { return tokenauth.WithScheme(scheme) }

// WithAllowAnonymous permits an outbound call whose token source returns an empty
// token to proceed without credentials. Without it an empty token fails closed, so
// a broken or absent credential source cannot silently downgrade authentication.
func WithAllowAnonymous() AuthOption { return tokenauth.WithAllowAnonymous() }

// Authentication returns client middleware that writes the token returned by token
// into the configured header of the outbound metadata.Info.Header, replacing
// any previous value and prefixed by the scheme.
//
// An empty token fails with errorx.Unauthenticated and reason
// ReasonTokenUnavailable unless WithAllowAnonymous was supplied, in which case the
// header is left untouched and the call proceeds unauthenticated.
//
// If the context carries no Info, or an Info with a nil Header, a fresh
// header is created and the next endpoint receives a context carrying it.
//
// If token fails with an error that already carries an *errorx.Error, that
// error is returned unchanged; any other error yields
// errorx.Unauthenticated with reason ReasonTokenUnavailable and the original
// error as its cause. The next endpoint is not called in either case.
// A nil token source panics during assembly.
func Authentication(token func(ctx context.Context) (string, error), opts ...AuthOption) endpoint.Middleware {
	if token == nil {
		panic("client: nil token source")
	}
	o := tokenauth.NewOptions(opts)
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			tok, err := token(ctx)
			if err != nil {
				if _, ok := errors.AsType[*errorx.Error](err); ok {
					return nil, err
				}
				return nil, errorx.New(errorx.Unauthenticated, ReasonTokenUnavailable, "obtain token").WithCause(err)
			}
			if tok == "" {
				if !o.AllowAnonymous {
					return nil, errorx.New(errorx.Unauthenticated, ReasonTokenUnavailable, "empty token")
				}
				return next(ctx, req)
			}
			info, ok := metadata.FromContext(ctx)
			if !ok {
				info.Kind = metadata.KindClient
			}
			if info.Header == nil {
				info.Header = make(metadata.MD)
				ctx = metadata.NewContext(ctx, info)
			}
			value := tok
			if o.Scheme != "" {
				value = o.Scheme + " " + tok
			}
			info.Header.Set(o.Header, value)
			return next(ctx, req)
		}
	}
}
