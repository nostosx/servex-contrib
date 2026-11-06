// Package tokenauth shares token header configuration without execution policies.
package tokenauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

const (
	ReasonMissingToken = "MISSING_TOKEN"
	ReasonInvalidToken = "INVALID_TOKEN"
	DefaultHeader      = "authorization"
	DefaultScheme      = "Bearer"
)

type Option func(*Options)

type Options struct {
	Header, Scheme string
	// AllowAnonymous lets an outbound call proceed without credentials. It is
	// opt-in so a missing token cannot silently downgrade authentication.
	AllowAnonymous bool
}

// WithHeader selects a case-insensitive header; empty keeps the default.
func WithHeader(name string) Option {
	return func(o *Options) {
		if name != "" {
			o.Header = strings.ToLower(name)
		}
	}
}

// WithScheme selects the prefix; empty treats the complete value as the token.
func WithScheme(scheme string) Option { return func(o *Options) { o.Scheme = scheme } }

// WithAllowAnonymous permits an outbound call that carries no credentials.
func WithAllowAnonymous() Option { return func(o *Options) { o.AllowAnonymous = true } }

func NewOptions(options []Option) Options {
	o := Options{Header: DefaultHeader, Scheme: DefaultScheme}
	for _, option := range options {
		if option != nil {
			option(&o)
		}
	}
	return o
}

func (o Options) Token(ctx context.Context) (string, error) {
	info, _ := metadata.FromContext(ctx)
	if len(info.Header.Values(o.Header)) > 1 {
		return "", errorx.New(errorx.Unauthenticated, ReasonInvalidToken, "multiple authorization values")
	}
	value := strings.TrimSpace(info.Header.Get(o.Header))
	if value == "" {
		return "", errorx.New(errorx.Unauthenticated, ReasonMissingToken, "missing token")
	}
	if o.Scheme == "" {
		return value, nil
	}
	scheme, token, _ := strings.Cut(value, " ")
	if !strings.EqualFold(scheme, o.Scheme) {
		return "", errorx.New(errorx.Unauthenticated, ReasonInvalidToken, "invalid authorization scheme")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", errorx.New(errorx.Unauthenticated, ReasonMissingToken, "missing token")
	}
	return token, nil
}

// StaticToken returns a verifier for one configured service credential. It
// compares fixed-size digests in constant time and never includes tokens in errors.
// Empty expected credentials always reject. Extraction remains Authentication's job.
func StaticToken(expected string) func(context.Context, string) (struct{}, error) {
	expectedHash := sha256.Sum256([]byte(expected))
	return func(_ context.Context, actual string) (struct{}, error) {
		actualHash := sha256.Sum256([]byte(actual))
		if expected == "" || subtle.ConstantTimeCompare(expectedHash[:], actualHash[:]) != 1 {
			return struct{}{}, errorx.New(errorx.Unauthenticated, ReasonInvalidToken, "invalid token")
		}
		return struct{}{}, nil
	}
}
