package httpx_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

func TestRouteFunc(t *testing.T) {
	s := httpx.NewServer()
	httpx.RouteFunc(s, "GET /v1/users/{id}", func(_ context.Context, req *userReq) (*userReply, error) {
		if req.ID == "0" {
			return nil, errorx.New(errorx.InvalidArgument, "INVALID_ID", "id must be positive")
		}
		return &userReply{ID: len(req.ID), Name: "ada" + req.ID}, nil
	}, httpx.WithOperation("users.Get"))

	w := do(s, httptest.NewRequest(http.MethodGet, "/v1/users/7", nil))
	if got := decodeJSON[userReply](t, w); w.Code != http.StatusOK || got.ID != 1 || got.Name != "ada7" {
		t.Errorf("status %d, reply %+v", w.Code, got)
	}
	w = do(s, httptest.NewRequest(http.MethodGet, "/v1/users/0", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("error status = %d, want 400", w.Code)
	}
}

func TestRouteFuncNilReply(t *testing.T) {
	s := httpx.NewServer()
	httpx.RouteFunc(s, "DELETE /v1/users/{id}", func(context.Context, *userReq) (*userReply, error) {
		return nil, nil
	})
	if w := do(s, httptest.NewRequest(http.MethodDelete, "/v1/users/7", nil)); w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
}

// A middleware that replaces the request with another type makes Typed fail
// with a TypeError, which the server reports as 500 Unknown.
func TestTypedMismatchIsInternal(t *testing.T) {
	swap := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, _ any) (any, error) { return next(ctx, "not a request") }
	}
	s := httpx.NewServer(httpx.WithMiddleware(swap), httpx.WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	httpx.RouteFunc(s, "GET /x", func(context.Context, *userReq) (*userReply, error) {
		t.Error("fn called with a mismatched request")
		return nil, nil
	})
	w := do(s, httptest.NewRequest(http.MethodGet, "/x", nil))
	body := decodeJSON[errBody](t, w)
	if w.Code != http.StatusInternalServerError || body.Code != "Unknown" {
		t.Errorf("status %d, body %+v; want 500 Unknown", w.Code, body)
	}
	if strings.Contains(w.Body.String(), "userReq") {
		t.Errorf("error body leaks type details: %s", w.Body)
	}
}

func TestServerWithName(t *testing.T) {
	if got := httpx.NewServer().Name(); got != "http" {
		t.Errorf("default Name = %q, want http", got)
	}
	if got := httpx.NewServer(httpx.WithName("admin-http")).Name(); got != "admin-http" {
		t.Errorf("Name = %q, want admin-http", got)
	}
}
