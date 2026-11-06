package client_test

import (
	"context"
	"errors"
	"testing"

	clientmiddleware "github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
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

// principalEcho returns the principal found in its context.
func principalEcho(ctx context.Context, _ any) (any, error) {
	u, ok := endpointmw.PrincipalFromContext[*user](ctx)
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

func TestClientInjectsHeader(t *testing.T) {
	header := make(metadata.MD)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Protocol: "http", Header: header})
	header.Set("authorization", "stale")

	var seen metadata.MD
	ep := clientmiddleware.Authentication(func(context.Context) (string, error) { return "tok", nil })(func(ctx context.Context, _ any) (any, error) {
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
	ep := clientmiddleware.Authentication(func(context.Context) (string, error) { return "tok", nil },
		clientmiddleware.WithTokenHeader("X-Token"), clientmiddleware.WithTokenScheme(""))(func(ctx context.Context, _ any) (any, error) {
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

func TestClientEmptyTokenFailsClosed(t *testing.T) {
	header := make(metadata.MD)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: header})
	called := false
	next := func(context.Context, any) (any, error) { called = true; return "ok", nil }

	_, err := clientmiddleware.Authentication(func(context.Context) (string, error) { return "", nil })(next)(ctx, nil)
	wantReason(t, err, errorx.Unauthenticated, clientmiddleware.ReasonTokenUnavailable)
	if called {
		t.Fatal("next endpoint called without credentials")
	}
	if len(header) != 0 {
		t.Fatalf("header = %v, want untouched", header)
	}
}

func TestClientEmptyTokenAllowedAnonymously(t *testing.T) {
	header := make(metadata.MD)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: header})
	ep := clientmiddleware.Authentication(
		func(context.Context) (string, error) { return "", nil },
		clientmiddleware.WithAllowAnonymous(),
	)(func(context.Context, any) (any, error) { return "ok", nil })
	if resp, err := ep(ctx, nil); err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
	if len(header) != 0 {
		t.Fatalf("header = %v, want untouched", header)
	}
}

func TestClientNilTokenSourcePanicsAtAssembly(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil token source did not panic")
		}
	}()
	clientmiddleware.Authentication(nil)
}

func TestClientTokenError(t *testing.T) {
	sentinel := errors.New("token endpoint down")
	called := false
	next := func(context.Context, any) (any, error) { called = true; return nil, nil }

	_, err := clientmiddleware.Authentication(func(context.Context) (string, error) { return "", sentinel })(next)(t.Context(), nil)
	wantReason(t, err, errorx.Unauthenticated, clientmiddleware.ReasonTokenUnavailable)
	if !errors.Is(err, sentinel) {
		t.Fatalf("cause lost: %v", err)
	}

	unavailable := errorx.New(errorx.Unavailable, "IDP_DOWN", "idp down")
	_, err = clientmiddleware.Authentication(func(context.Context) (string, error) { return "", unavailable })(next)(t.Context(), nil)
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
	server := endpointmw.Authentication(verifyUser)(principalEcho)
	transport := func(ctx context.Context, req any) (any, error) {
		out, _ := metadata.FromContext(ctx)
		in := metadata.NewContext(context.WithoutCancel(ctx), metadata.Info{Kind: metadata.KindServer, Header: out.Header.Clone()})
		return server(in, req)
	}
	client := endpoint.Chain(clientmiddleware.Authentication(func(context.Context) (string, error) { return "good", nil }))(transport)
	resp, err := client(metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: make(metadata.MD)}), nil)
	if err != nil || resp.(*user).ID != "u1" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}
