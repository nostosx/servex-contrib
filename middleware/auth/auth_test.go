package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex/endpoint"
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
	u, ok := auth.FromContext[*user](ctx)
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

func TestServerRoundTrip(t *testing.T) {
	ep := auth.New(verifyUser)(principalEcho)
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

func TestFromContextTypeIsolation(t *testing.T) {
	ctx := auth.NewContext(t.Context(), "alice")
	if _, ok := auth.FromContext[*user](ctx); ok {
		t.Fatal("FromContext[*user] found a string principal")
	}
	if s, ok := auth.FromContext[string](ctx); !ok || s != "alice" {
		t.Fatalf("FromContext[string] = %q, %v", s, ok)
	}
	if _, ok := auth.FromContext[string](nil); ok { //nolint:staticcheck // nil ctx is tolerated
		t.Fatal("FromContext(nil) reported a principal")
	}
	if _, ok := auth.FromContext[string](t.Context()); ok {
		t.Fatal("empty context reported a principal")
	}
}

func TestServerFailures(t *testing.T) {
	called := false
	next := func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	}
	ep := auth.New(verifyUser)(next)
	cases := []struct {
		name   string
		ctx    context.Context
		reason string
	}{
		{"no info", t.Context(), auth.ReasonMissingToken},
		{"no header", serverCtx(t), auth.ReasonMissingToken},
		{"empty header", serverCtx(t, "authorization", "  "), auth.ReasonMissingToken},
		{"scheme only", serverCtx(t, "authorization", "Bearer "), auth.ReasonMissingToken},
		{"wrong scheme", serverCtx(t, "authorization", "Basic abc"), auth.ReasonInvalidToken},
		{"no scheme", serverCtx(t, "authorization", "good"), auth.ReasonInvalidToken},
		{"rejected", serverCtx(t, "authorization", "Bearer bad"), auth.ReasonInvalidToken},
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

func TestServerVerifyCauseAndErrorx(t *testing.T) {
	sentinel := errors.New("expired")
	ep := auth.New(func(context.Context, string) (string, error) { return "", sentinel })(principalEcho)
	_, err := ep(serverCtx(t, "authorization", "Bearer x"), nil)
	wantReason(t, err, errorx.Unauthenticated, auth.ReasonInvalidToken)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want cause kept", err)
	}

	denied := errorx.New(errorx.PermissionDenied, "NO_ACCESS", "no access")
	ep = auth.New(func(context.Context, string) (string, error) { return "", denied })(principalEcho)
	if _, err := ep(serverCtx(t, "authorization", "Bearer x"), nil); err != denied {
		t.Fatalf("err = %v, want errorx error unchanged", err)
	}
}

func TestServerCustomHeaderAndScheme(t *testing.T) {
	var gotToken string
	verify := func(_ context.Context, token string) (string, error) {
		gotToken = token
		return "svc", nil
	}
	ep := auth.New(verify, auth.WithHeader("X-API-Key"), auth.WithScheme(""))(func(ctx context.Context, _ any) (any, error) {
		p, _ := auth.FromContext[string](ctx)
		return p, nil
	})
	resp, err := ep(serverCtx(t, "x-api-key", "k 123"), nil)
	if err != nil || resp != "svc" || gotToken != "k 123" {
		t.Fatalf("got (%v, %v), token %q", resp, err, gotToken)
	}

	ep = auth.New(verify, auth.WithScheme("Token"), auth.WithHeader(""))(func(context.Context, any) (any, error) { return nil, nil })
	if _, err := ep(serverCtx(t, "authorization", "token abc"), nil); err != nil || gotToken != "abc" {
		t.Fatalf("err = %v, token %q", err, gotToken)
	}
}

func TestClientInjectsHeader(t *testing.T) {
	header := make(metadata.MD)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Protocol: "http", Header: header})
	header.Set("authorization", "stale")

	var seen metadata.MD
	ep := auth.Client(func(context.Context) (string, error) { return "tok", nil })(func(ctx context.Context, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		seen = info.Header
		return nil, nil
	})
	if _, err := ep(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := header.Values("authorization"); len(got) != 1 || got[0] != "Bearer tok" {
		t.Fatalf("header = %v", got)
	}
	if seen.Get("authorization") != "Bearer tok" {
		t.Fatalf("next saw %v", seen)
	}
}

func TestClientCreatesHeader(t *testing.T) {
	var info metadata.Info
	ep := auth.Client(func(context.Context) (string, error) { return "tok", nil },
		auth.WithHeader("X-Token"), auth.WithScheme(""))(func(ctx context.Context, _ any) (any, error) {
		info, _ = metadata.FromContext(ctx)
		return nil, nil
	})
	if _, err := ep(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if info.Kind != metadata.KindClient || info.Header.Get("x-token") != "tok" {
		t.Fatalf("info = %+v", info)
	}

	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "op"})
	if _, err := ep(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if info.Operation != "op" || info.Header.Get("x-token") != "tok" {
		t.Fatalf("info = %+v", info)
	}
}

func TestClientEmptyToken(t *testing.T) {
	header := make(metadata.MD)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: header})
	ep := auth.Client(func(context.Context) (string, error) { return "", nil })(func(context.Context, any) (any, error) { return "ok", nil })
	if resp, err := ep(ctx, nil); err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
	if len(header) != 0 {
		t.Fatalf("header = %v, want untouched", header)
	}
}

func TestClientTokenError(t *testing.T) {
	sentinel := errors.New("token endpoint down")
	called := false
	next := func(context.Context, any) (any, error) { called = true; return nil, nil }

	_, err := auth.Client(func(context.Context) (string, error) { return "", sentinel })(next)(t.Context(), nil)
	wantReason(t, err, errorx.Unauthenticated, auth.ReasonTokenUnavailable)
	if !errors.Is(err, sentinel) {
		t.Fatalf("cause lost: %v", err)
	}

	unavailable := errorx.New(errorx.Unavailable, "IDP_DOWN", "idp down")
	_, err = auth.Client(func(context.Context) (string, error) { return "", unavailable })(next)(t.Context(), nil)
	if err != unavailable {
		t.Fatalf("err = %v, want errorx error unchanged", err)
	}
	if called {
		t.Fatal("next endpoint called")
	}
}

func TestClientToServer(t *testing.T) {
	// The client writes the outbound header; a server reading the same
	// header as its inbound header authenticates the call.
	server := auth.New(verifyUser)(principalEcho)
	transport := func(ctx context.Context, req any) (any, error) {
		out, _ := metadata.FromContext(ctx)
		in := metadata.NewContext(context.WithoutCancel(ctx), metadata.Info{Kind: metadata.KindServer, Header: out.Header.Clone()})
		return server(in, req)
	}
	client := endpoint.Chain(auth.Client(func(context.Context) (string, error) { return "good", nil }))(transport)
	resp, err := client(metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: make(metadata.MD)}), nil)
	if err != nil || resp.(*user).ID != "u1" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}
