package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/health"
)

func TestHTTPMiddlewareWrapsEveryRequest(t *testing.T) {
	var order []string
	mark := func(name string) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	endpointMW := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			order = append(order, "endpoint middleware")
			return next(ctx, req)
		}
	}
	s := httpx.NewServer(
		httpx.WithHTTPMiddleware(mark("outer"), nil),
		httpx.WithHTTPMiddleware(mark("inner")),
		httpx.WithMiddleware(endpointMW),
		httpx.WithHealth(health.CheckerFunc(func(context.Context) error { return nil })),
	)
	httpx.Route[userReq](s, "POST /v1/users", func(context.Context, any) (any, error) {
		order = append(order, "handler")
		return nil, nil
	})
	s.HandleHTTP("GET /raw", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		order = append(order, "raw")
		w.WriteHeader(http.StatusTeapot)
	}))

	for _, tc := range []struct {
		name   string
		req    *http.Request
		status int
		order  []string
	}{
		{"route", httptest.NewRequest(http.MethodPost, "/v1/users", nil), http.StatusNoContent,
			[]string{"outer", "inner", "endpoint middleware", "handler"}},
		{"raw handler", httptest.NewRequest(http.MethodGet, "/raw", nil), http.StatusTeapot,
			[]string{"outer", "inner", "raw"}},
		{"health", httptest.NewRequest(http.MethodGet, "/livez", nil), http.StatusOK,
			[]string{"outer", "inner"}},
		{"unmatched", httptest.NewRequest(http.MethodGet, "/missing", nil), http.StatusNotFound,
			[]string{"outer", "inner"}},
		{"binding failure", jsonRequest(http.MethodPost, "/v1/users", `{"age":"x"}`), http.StatusBadRequest,
			[]string{"outer", "inner"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			order = nil
			if w := do(s, tc.req); w.Code != tc.status {
				t.Fatalf("status = %d, want %d; body = %s", w.Code, tc.status, w.Body)
			}
			if !slices.Equal(order, tc.order) {
				t.Errorf("order = %v, want %v", order, tc.order)
			}
		})
	}
}

func TestHTTPMiddlewareRejectsBeforeBodyIsRead(t *testing.T) {
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		})
	}
	s := httpx.NewServer(httpx.WithHTTPMiddleware(deny))
	s.Handle("POST /", func(context.Context, any) (any, error) {
		t.Error("endpoint called")
		return nil, nil
	}, func() any {
		t.Error("request factory called")
		return new(userReq)
	})
	body := new(observedLimitBody)
	r := httptest.NewRequest(http.MethodPost, "/", body)
	r.Header.Set("Content-Type", "application/json")
	if w := do(s, r); w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if body.reads != 0 {
		t.Errorf("body reads = %d, want zero", body.reads)
	}
}

// jsonRequest builds a request with a JSON body.
func jsonRequest(method, target, body string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}
