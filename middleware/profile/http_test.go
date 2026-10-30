package profile

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nostosx/servex/metadata"
)

func TestNativeHTTPComposition(t *testing.T) {
	p := New(Config{})
	handler := p.HTTP("browser")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info, ok := metadata.FromContext(r.Context())
		if !ok || info.Operation != "browser" {
			t.Error(info)
		}
		if r.URL.Path == "/panic" {
			panic("private panic")
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	for _, test := range []struct {
		path string
		want int
	}{{"/forbidden", http.StatusForbidden}, {"/panic", http.StatusInternalServerError}} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "https://example.com"+test.path, nil))
		if w.Code != test.want {
			t.Fatal(test.path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	stream := p.HTTP("events")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
	}))
	stream.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "https://example.com/events", nil))
	if !w.Flushed {
		t.Fatal("flush capability lost")
	}
}
