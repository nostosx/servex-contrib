package httpx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	httptransport "github.com/nostosx/servex/transport/http"
)

func TestServerConfigDecodeAndDefaults(t *testing.T) {
	cfg := DefaultServerConfig()
	if err := json.Unmarshal([]byte(`{"name":"public","address":"127.0.0.1:0","read_header_timeout":5000000000,"max_body_bytes":0}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ReadHeaderTimeout != 5*time.Second || cfg.MaxBodyBytes != 0 || cfg.MaxHeaderBytes != 0 {
		t.Fatalf("decoded configuration = %+v", cfg)
	}
	srv, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if srv.Name() != "public" || srv.addr != cfg.Address || srv.readHeaderTimeout != cfg.ReadHeaderTimeout || srv.maxBodyBytes != 0 {
		t.Fatalf("effective configuration = %+v", srv)
	}
	if srv.srv != nil || srv.URL() != nil {
		t.Fatal("construction opened a listener")
	}
	if got := DefaultServerConfig(); got.Name != "http" || got.Address != ":8000" || got.MaxBodyBytes != DefaultMaxBodyBytes {
		t.Fatalf("defaults = %+v", got)
	}
}

func TestServerConfigValidate(t *testing.T) {
	for _, address := range []string{":0", "localhost:8080", "127.0.0.1:0", "[::1]:0", "[fe80::1%lo0]:9000"} {
		t.Run(address, func(t *testing.T) {
			cfg := DefaultServerConfig()
			cfg.Address = address
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, tt := range []struct {
		name   string
		change func(*ServerConfig)
	}{
		{"name", func(c *ServerConfig) { c.Name = " " }},
		{"missing_port", func(c *ServerConfig) { c.Address = "localhost" }},
		{"named_port", func(c *ServerConfig) { c.Address = "localhost:http" }},
		{"negative_port", func(c *ServerConfig) { c.Address = "localhost:-1" }},
		{"oversized_port", func(c *ServerConfig) { c.Address = "localhost:65536" }},
		{"whitespace", func(c *ServerConfig) { c.Address = " localhost:80" }},
		{"path", func(c *ServerConfig) { c.Address = "host/path:80" }},
		{"invalid_ip", func(c *ServerConfig) { c.Address = "[::gg]:80" }},
		{"read_timeout", func(c *ServerConfig) { c.ReadTimeout = -1 }},
		{"header_timeout", func(c *ServerConfig) { c.ReadHeaderTimeout = -1 }},
		{"write_timeout", func(c *ServerConfig) { c.WriteTimeout = -1 }},
		{"idle_timeout", func(c *ServerConfig) { c.IdleTimeout = -1 }},
		{"header_limit", func(c *ServerConfig) { c.MaxHeaderBytes = -1 }},
		{"body_limit", func(c *ServerConfig) { c.MaxBodyBytes = -1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultServerConfig()
			tt.change(&cfg)
			called := false
			srv, err := NewServerFromConfig(cfg, func(*serverOptions) { called = true })
			if srv != nil || err == nil || called {
				t.Fatalf("invalid configuration: server=%v error=%v optionCalled=%v", srv, err, called)
			}
		})
	}
}

func TestServerConfigRuntimeOverrides(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Address = "127.0.0.1:0"
	assembled := false
	middleware := WithHTTPMiddleware(func(next http.Handler) http.Handler {
		assembled = true
		return next
	})
	if srv, err := NewServerFromConfig(cfg, WithReadTimeout(-time.Second), middleware); srv != nil || err == nil || assembled {
		t.Fatalf("invalid effective configuration: server=%v error=%v middleware=%v", srv, err, assembled)
	}
	srv, err := NewServerFromConfig(cfg, WithName("override"), WithIdleTimeout(time.Minute), middleware)
	if err != nil || !assembled || srv.Name() != "override" || srv.idleTimeout != time.Minute {
		t.Fatalf("valid runtime overrides: server=%v error=%v middleware=%v", srv, err, assembled)
	}
}

func TestServerConfigBodyLimitAndNetworkSettings(t *testing.T) {
	for _, limit := range []int64{0, 4} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			cfg := DefaultServerConfig()
			cfg.Address = "127.0.0.1:0"
			cfg.ReadTimeout, cfg.ReadHeaderTimeout = 10*time.Second, 2*time.Second
			cfg.WriteTimeout, cfg.IdleTimeout = 10*time.Second, time.Minute
			cfg.MaxHeaderBytes, cfg.MaxBodyBytes = 8192, limit
			srv, err := NewServerFromConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			httptransport.RouteFunc(srv, "POST /echo", func(_ context.Context, req *struct {
				Value string `json:"value"`
			}) (string, error) {
				return req.Value, nil
			})
			if err := srv.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := srv.Stop(ctx); err != nil {
					t.Error(err)
				}
			})
			if srv.srv.ReadTimeout != cfg.ReadTimeout || srv.srv.ReadHeaderTimeout != cfg.ReadHeaderTimeout || srv.srv.WriteTimeout != cfg.WriteTimeout || srv.srv.IdleTimeout != cfg.IdleTimeout || srv.srv.MaxHeaderBytes != cfg.MaxHeaderBytes {
				t.Fatal("configured net/http settings were not applied")
			}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL().String()+"/echo", strings.NewReader(`{"value":"ok"}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			want := http.StatusOK
			if limit > 0 {
				want = http.StatusRequestEntityTooLarge
			}
			if response.StatusCode != want {
				t.Fatalf("status=%d body=%s want=%d", response.StatusCode, body, want)
			}
		})
	}
}
