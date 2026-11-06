package endpoint_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

type user struct {
	ID    string
	Roles []string
}

func verifyUser(_ context.Context, token string) (*user, error) {
	if token == "good" {
		return &user{ID: "u1", Roles: []string{"admin"}}, nil
	}
	return nil, errors.New("bad signature")
}

func serverCtx(t *testing.T, kv ...string) context.Context {
	return metadata.NewContext(t.Context(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /me", Header: metadata.Pairs(kv...),
	})
}

// principalEcho returns the principal found in its context.
func principalEcho(ctx context.Context, _ any) (any, error) {
	u, ok := endpoint.PrincipalFromContext[*user](ctx)
	if !ok {
		return nil, errors.New("no principal")
	}
	return u, nil
}

func wantReason(t *testing.T, err error, code errorx.Code, reason string) {
	t.Helper()
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != code || e.Reason != reason {
		t.Fatalf("err = %v, want %v/%s", err, code, reason)
	}
}

func TestAuthenticationServerRoundTrip(t *testing.T) {
	ep := endpoint.Authentication(verifyUser)(principalEcho)
	for _, header := range []string{"Bearer good", "bearer good", "BEARER   good  "} {
		resp, err := ep(serverCtx(t, "Authorization", header), nil)
		if err != nil {
			t.Fatalf("%q: %v", header, err)
		}
		if u := resp.(*user); u.ID != "u1" || u.Roles[0] != "admin" {
			t.Fatalf("%q: principal = %+v", header, u)
		}
	}
}

func TestAuthenticationFromContextTypeIsolation(t *testing.T) {
	ctx := endpoint.NewPrincipalContext(t.Context(), "alice")
	if _, ok := endpoint.PrincipalFromContext[*user](ctx); ok {
		t.Fatal("FromContext[*user] found a string principal")
	}
	if s, ok := endpoint.PrincipalFromContext[string](ctx); !ok || s != "alice" {
		t.Fatalf("FromContext[string] = %q, %v", s, ok)
	}
	if _, ok := endpoint.PrincipalFromContext[string](nil); ok { //nolint:staticcheck // nil ctx is tolerated
		t.Fatal("FromContext(nil) reported a principal")
	}
	if _, ok := endpoint.PrincipalFromContext[string](t.Context()); ok {
		t.Fatal("empty context reported a principal")
	}
}

func TestAuthenticationServerFailures(t *testing.T) {
	called := false
	next := func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	}
	ep := endpoint.Authentication(verifyUser)(next)
	cases := []struct {
		name   string
		ctx    context.Context
		reason string
	}{
		{"no info", t.Context(), endpoint.ReasonMissingToken},
		{"no header", serverCtx(t), endpoint.ReasonMissingToken},
		{"empty header", serverCtx(t, "authorization", "  "), endpoint.ReasonMissingToken},
		{"scheme only", serverCtx(t, "authorization", "Bearer "), endpoint.ReasonMissingToken},
		{"wrong scheme", serverCtx(t, "authorization", "Basic abc"), endpoint.ReasonInvalidToken},
		{"no scheme", serverCtx(t, "authorization", "good"), endpoint.ReasonInvalidToken},
		{"rejected", serverCtx(t, "authorization", "Bearer bad"), endpoint.ReasonInvalidToken},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			_, err := ep(tc.ctx, nil)
			wantReason(t, err, errorx.Unauthenticated, tc.reason)
			if called {
				t.Fatal("next endpoint called")
			}
		})
	}
}

func TestAuthenticationServerVerifyCauseAndErrorx(t *testing.T) {
	sentinel := errors.New("expired")
	ep := endpoint.Authentication(func(context.Context, string) (string, error) { return "", sentinel })(principalEcho)
	_, err := ep(serverCtx(t, "authorization", "Bearer x"), nil)
	wantReason(t, err, errorx.Unauthenticated, endpoint.ReasonInvalidToken)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want cause kept", err)
	}

	denied := errorx.New(errorx.PermissionDenied, "NO_ACCESS", "no access")
	ep = endpoint.Authentication(func(context.Context, string) (string, error) { return "", denied })(principalEcho)
	if _, err := ep(serverCtx(t, "authorization", "Bearer x"), nil); err != denied {
		t.Fatalf("err = %v, want errorx error unchanged", err)
	}
}

func TestAuthenticationServerCustomHeaderAndScheme(t *testing.T) {
	var gotToken string
	verify := func(_ context.Context, token string) (string, error) {
		gotToken = token
		return "svc", nil
	}
	ep := endpoint.Authentication(verify, endpoint.WithTokenHeader("X-API-Key"), endpoint.WithTokenScheme(""))(func(ctx context.Context, _ any) (any, error) {
		p, _ := endpoint.PrincipalFromContext[string](ctx)
		return p, nil
	})
	resp, err := ep(serverCtx(t, "x-api-key", "k 123"), nil)
	if err != nil || resp != "svc" || gotToken != "k 123" {
		t.Fatalf("got (%v, %v), token %q", resp, err, gotToken)
	}

	ep = endpoint.Authentication(verify, endpoint.WithTokenScheme("Token"), endpoint.WithTokenHeader(""))(func(context.Context, any) (any, error) { return nil, nil })
	if _, err := ep(serverCtx(t, "authorization", "token abc"), nil); err != nil || gotToken != "abc" {
		t.Fatalf("err = %v, token %q", err, gotToken)
	}
}
