package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
)

func TestBindingCacheSupportsDynamicFactoryTypes(t *testing.T) {
	type first struct {
		ID string `json:"id"`
	}
	type second struct {
		Prefix string `json:"prefix"`
		ID     string `json:"id"`
	}
	calls := 0
	s := httpx.NewServer()
	s.Handle("GET /items/{id}", func(_ context.Context, req any) (any, error) {
		var id string
		switch req := req.(type) {
		case *first:
			id = req.ID
		case *second:
			if req.Prefix != "" {
				t.Fatalf("bound ID to wrong field: %+v", req)
			}
			id = req.ID
		}
		return struct {
			ID string `json:"id"`
		}{ID: id}, nil
	}, func() any {
		calls++
		if calls%2 == 1 {
			return new(first)
		}
		return new(second)
	})
	if calls != 0 {
		t.Fatal("request factory called during registration")
	}
	for range 4 {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/items/42?id=query", nil))
		if w.Code != http.StatusOK || w.Body.String() != `{"id":"42"}` {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
	if calls != 4 {
		t.Fatalf("factory called %d times for four requests", calls)
	}
}
