package grpcx

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/health"
)

func TestServerConfigDecodeAndDefaults(t *testing.T) {
	cfg := DefaultServerConfig()
	if err := json.Unmarshal([]byte(`{"name":"public","address":"127.0.0.1:0","max_recv_msg_size":0,"max_send_msg_size":0,"health_interval":1000000000}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRecvMsgSize != 0 || cfg.MaxSendMsgSize != 0 || cfg.HealthInterval != time.Second {
		t.Fatalf("decoded configuration = %+v", cfg)
	}
	srv, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if srv.Name() != "public" || srv.addr != cfg.Address || srv.URL() != nil {
		t.Fatal("configured identity/address were ignored or construction listened")
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := DefaultServerConfig(); got.Name != "grpc" || got.Address != ":9000" || got.MaxRecvMsgSize != 4<<20 || got.MaxSendMsgSize != math.MaxInt32 || got.HealthInterval != defaultHealthInterval {
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
		{"receive_limit", func(c *ServerConfig) { c.MaxRecvMsgSize = -1 }},
		{"send_limit", func(c *ServerConfig) { c.MaxSendMsgSize = -1 }},
		{"health_interval", func(c *ServerConfig) { c.HealthInterval = 0 }},
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
	for _, option := range []ServerOption{
		WithAddress("invalid"),
		func(o *serverOptions) { o.healthInterval = 0 },
	} {
		if srv, err := NewServerFromConfig(cfg, option); srv != nil || err == nil {
			t.Fatalf("invalid effective configuration accepted: server=%v error=%v", srv, err)
		}
	}
	srv, err := NewServerFromConfig(cfg, WithName("override"), WithAddress("localhost:0"))
	if err != nil || srv.Name() != "override" || srv.addr != "localhost:0" {
		t.Fatalf("valid runtime overrides: server=%v error=%v", srv, err)
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestServerConfigMessageLimits(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receive int
		send    int
		service string
		want    codes.Code
	}{
		{"zero_defaults", 0, 0, "", codes.OK},
		{"receive", 1, 0, "grpc.health.v1.Health", codes.ResourceExhausted},
		{"send", 0, 1, "", codes.ResourceExhausted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultServerConfig()
			cfg.Address = "127.0.0.1:0"
			cfg.MaxRecvMsgSize, cfg.MaxSendMsgSize = tt.receive, tt.send
			srv, err := NewServerFromConfig(cfg,
				WithHealth(health.CheckerFunc(func(context.Context) error { return nil })),
				// Typed configuration remains authoritative over contradictory native limits.
				WithGRPCOptions(grpc.MaxRecvMsgSize(4096), grpc.MaxSendMsgSize(4096)),
			)
			if err != nil {
				t.Fatal(err)
			}
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
			conn, err := grpc.NewClient(srv.URL().Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: tt.service})
			if code := status.Code(err); code != tt.want {
				t.Fatalf("code=%v error=%v want=%v", code, err, tt.want)
			}
		})
	}
}
