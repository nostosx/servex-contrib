package httpx

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestRestrictedHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/large", http.StatusFound)
			return
		}
		if _, err := w.Write([]byte(strings.Repeat("x", 32))); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := NewRestrictedHTTPClient(HTTPPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if res, err := client.Get(server.URL); err == nil {
		res.Body.Close()
		t.Fatal("default client reached loopback")
	}
	client, err = NewRestrictedHTTPClient(HTTPPolicy{AllowPrivateNetwork: true, MaxResponseBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	res, err := client.Get(server.URL + "/redirect")
	if err == nil {
		defer res.Body.Close()
		_, err = io.ReadAll(res.Body)
	}
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("large redirected body: %v", err)
	}
}

func TestPublicAddresses(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "::ffff:127.0.0.1", "::1", "169.254.169.254", "100.64.0.1", "192.0.2.1", "198.19.0.1", "fc00::1", "2001:db8::1", "64:ff9b::a00:1", "2002:7f00:1::", "3fff::1"} {
		if publicAddress(netip.MustParseAddr(raw)) {
			t.Fatal(raw)
		}
	}
	if !publicAddress(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("public IP rejected")
	}
}
