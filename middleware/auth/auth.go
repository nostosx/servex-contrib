package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// Reasons of the errors returned by the middleware.
const (
	// ReasonMissingToken means the call carried no token.
	ReasonMissingToken = "MISSING_TOKEN"
	// ReasonInvalidToken means the token was malformed or rejected by verify.
	ReasonInvalidToken = "INVALID_TOKEN"
	// ReasonTokenUnavailable means the client could not obtain a token.
	ReasonTokenUnavailable = "TOKEN_UNAVAILABLE"
)

// Default header and scheme.
const (
	DefaultHeader = "authorization"
	DefaultScheme = "Bearer"
)

// Option configures the middleware returned by New and Client.
type Option func(*options)

type options struct {
	header string
	scheme string
}

// WithHeader sets the header carrying the token. The default is
// DefaultHeader; an empty name keeps the default. Header names are
// case-insensitive.
func WithHeader(name string) Option {
	return func(o *options) {
		if name != "" {
			o.header = strings.ToLower(name)
		}
	}
}

// WithScheme sets the authentication scheme preceding the token, as in
// "Bearer <token>". The default is DefaultScheme. An empty scheme means the
// whole header value is the token.
func WithScheme(s string) Option {
	return func(o *options) { o.scheme = s }
}

func newOptions(opts []Option) options {
	o := options{header: DefaultHeader, scheme: DefaultScheme}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// principalKey is the context key under which a principal of type P is
// stored; each P has its own key.
type principalKey[P any] struct{}

// NewContext returns a copy of ctx carrying the principal p, as New does
// after a successful verification. It is useful for testing handlers that
// call FromContext.
func NewContext[P any](ctx context.Context, p P) context.Context {
	return context.WithValue(ctx, principalKey[P]{}, p)
}

// FromContext returns the principal of type P stored in ctx by New, if any.
// P must be the same type argument as the one used with New.
func FromContext[P any](ctx context.Context) (P, bool) {
	if ctx == nil {
		var zero P
		return zero, false
	}
	p, ok := ctx.Value(principalKey[P]{}).(P)
	return p, ok
}

// New returns server middleware that authenticates calls with verify.
//
// The token is read from the configured header of the metadata.Info in the
// context and the scheme prefix, compared case-insensitively, is removed.
// A missing Info, header or token yields errorx.Unauthenticated with reason
// ReasonMissingToken; a value with a different scheme yields reason
// ReasonInvalidToken. verify is then called with the token; on success its
// principal is stored in the context passed to the next endpoint, where
// FromContext[P] returns it.
//
// If verify fails with an error that already carries an *errorx.Error, for
// example errorx.PermissionDenied or errorx.Unavailable when a key server is
// down, that error is returned unchanged. Any other error yields
// errorx.Unauthenticated with reason ReasonInvalidToken and the original
// error as its cause.
func New[P any](verify func(ctx context.Context, token string) (P, error), opts ...Option) endpoint.Middleware {
	o := newOptions(opts)
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			token, err := o.token(ctx)
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
			return next(NewContext(ctx, p), req)
		}
	}
}

// token extracts the token from the inbound header in ctx.
func (o *options) token(ctx context.Context) (string, error) {
	info, _ := metadata.FromContext(ctx)
	value := strings.TrimSpace(info.Header.Get(o.header))
	if value == "" {
		return "", errorx.New(errorx.Unauthenticated, ReasonMissingToken, "missing token")
	}
	if o.scheme == "" {
		return value, nil
	}
	scheme, token, _ := strings.Cut(value, " ")
	if !strings.EqualFold(scheme, o.scheme) {
		return "", errorx.New(errorx.Unauthenticated, ReasonInvalidToken, "invalid authorization scheme")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", errorx.New(errorx.Unauthenticated, ReasonMissingToken, "missing token")
	}
	return token, nil
}

// Client returns client middleware that writes the token returned by token
// into the configured header of the outbound metadata.Info.Header, replacing
// any previous value and prefixed by the scheme. An empty token leaves the
// header untouched.
//
// If the context carries no Info, or an Info with a nil Header, a fresh
// header is created and the next endpoint receives a context carrying it.
//
// If token fails with an error that already carries an *errorx.Error, that
// error is returned unchanged; any other error yields
// errorx.Unauthenticated with reason ReasonTokenUnavailable and the original
// error as its cause. The next endpoint is not called in either case.
func Client(token func(ctx context.Context) (string, error), opts ...Option) endpoint.Middleware {
	o := newOptions(opts)
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
			if o.scheme != "" {
				value = o.scheme + " " + tok
			}
			info.Header.Set(o.header, value)
			return next(ctx, req)
		}
	}
}
