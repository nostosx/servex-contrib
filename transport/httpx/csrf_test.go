package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Standard-library protection composes outside the HTTP transport. Unsafe
// cross-site browser requests are rejected before application handlers run.
func TestStandardCrossOriginProtection(t *testing.T) {
	handler := http.NewCrossOriginProtection().Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, test := range []struct {
		method, origin string
		status         int
	}{
		{"POST", "https://attacker.example", http.StatusForbidden},
		{"POST", "https://api.example", http.StatusNoContent},
		{"GET", "https://attacker.example", http.StatusNoContent},
	} {
		r := httptest.NewRequest(test.method, "https://api.example/resource", nil)
		r.Header.Set("Origin", test.origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("%s %s: %d", test.method, test.origin, w.Code)
		}
	}
}
