package consul

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnectionConfig(t *testing.T) {
	for _, tc := range []struct {
		name, address, scheme string
		valid                 bool
	}{
		{"default", "127.0.0.1:8500", "http", true},
		{"TLS IPv6", "[::1]:8501", "https", true},
		{"DNS", "consul.example:8500", "https", true},
		{"missing host", ":8500", "http", false},
		{"missing port", "localhost", "http", false},
		{"non-numeric port", "localhost:service", "http", false},
		{"port overflow", "localhost:65536", "http", false},
		{"zero port", "localhost:0", "http", false},
		{"URL in address", "https://localhost:8500", "https", false},
		{"credentials in address", "user@localhost:8500", "http", false},
		{"unsupported scheme", "localhost:8500", "ftp", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Address, cfg.Scheme = tc.address, tc.scheme
			_, err := NewFromConfig(cfg)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestConnectionConfigPassesCredentialsAndDatacenter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Consul-Token") != "fixture-acl-token" || r.URL.Query().Get("dc") != "fixture-dc" {
			t.Error("connection configuration was not applied to the SDK request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.Address = strings.TrimPrefix(server.URL, "http://")
	cfg.ACLToken, cfg.Datacenter = "fixture-acl-token", "fixture-dc"
	registry, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.client.Catalog().Services(nil); err != nil {
		t.Fatal(err)
	}
}
