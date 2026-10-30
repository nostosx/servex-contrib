package cors

import (
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/rs/cors"
)

func TestBrowserPreflightAndCredentials(t *testing.T) {
	options := sdk.Options{AllowedOrigins: []string{"https://app.example"}, AllowedMethods: []string{"POST"}, AllowedHeaders: []string{"Content-Type"}, AllowCredentials: true}
	wrap, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	options.AllowedOrigins[0] = "https://attacker.example"
	calls := 0
	handler := wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusOK) }))
	for _, origin := range []string{"https://app.example", "https://attacker.example"} {
		r := httptest.NewRequest(http.MethodOptions, "https://api.example/resource", nil)
		r.Header.Set("Origin", origin)
		r.Header.Set("Access-Control-Request-Method", "POST")
		r.Header.Set("Access-Control-Request-Headers", "content-type")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		allowed := w.Header().Get("Access-Control-Allow-Origin")
		if origin == "https://app.example" && (allowed != origin || w.Header().Get("Access-Control-Allow-Credentials") != "true") {
			t.Fatal(w.Header())
		}
		if origin != "https://app.example" && allowed != "" {
			t.Fatal("untrusted origin accepted")
		}
	}
	if calls != 0 {
		t.Fatal("preflight reached application")
	}
	for _, options := range []sdk.Options{{}, {AllowedOrigins: []string{"*"}, AllowCredentials: true}, {AllowedOrigins: []string{""}}} {
		if _, err := New(options); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}
