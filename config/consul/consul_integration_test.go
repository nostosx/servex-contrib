//go:build integration

package consul_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex-contrib/config/consul"
	"github.com/nostosx/servex/config"
)

// integrationClient connects to the Consul agent at SERVEX_IT_CONSUL_ADDR
// (host:port of the HTTP API) and skips the test when it is unset.
func integrationClient(t *testing.T) *api.Client {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_CONSUL_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_CONSUL_ADDR not set")
	}
	cfg := api.DefaultConfig()
	cfg.Address = addr
	c, err := api.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestIntegrationLoadAndWatch(t *testing.T) {
	c := integrationClient(t)
	prefix := "servex-it/" + t.Name() + "/" + time.Now().Format("150405.000000") + "/"
	kv := c.KV()
	write := func(key, value string) {
		t.Helper()
		if _, err := kv.Put(&api.KVPair{Key: prefix + key, Value: []byte(value)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if _, err := kv.DeleteTree(prefix, nil); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	write("http/addr", `":8000"`)
	write("http/timeout", `"1s"`)
	write("db", `{"dsn":"x","pool":5}`)
	write("v", `0`)

	single := consul.New(c, prefix+"db")
	got, err := single.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"dsn": "x", "pool": 5.0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("single Load = %#v, want %#v", got, want)
	}

	cfg, err := config.New(t.Context(), consul.New(c, prefix, consul.WithPrefix(), consul.WithWaitTime(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Duration("http.timeout"); got != time.Second {
		t.Fatalf("http.timeout = %v", got)
	}
	if got := cfg.Int("db.pool"); got != 5 {
		t.Fatalf("db.pool = %d", got)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reloaded := make(chan int, 100)
	done := make(chan error, 1)
	go func() {
		done <- cfg.Watch(ctx, func(c *config.Config) { reloaded <- c.Int("v") })
	}()
	write("http/timeout", `"2s"`)
	write("v", `1`)
	deadline := time.After(30 * time.Second)
	for {
		select {
		case v := <-reloaded:
			if v != 1 {
				continue
			}
			if got := cfg.Duration("http.timeout"); got != 2*time.Second {
				t.Fatalf("http.timeout = %v after reload", got)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Watch = %v", err)
			}
			return
		case <-deadline:
			t.Fatal("no reload")
		}
	}
}
