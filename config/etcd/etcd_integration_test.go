//go:build integration

package etcd_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex-contrib/config/etcd"
	"github.com/nostosx/servex/config"
)

// TestIntegrationLoadAndWatch runs against the etcd cluster listed in
// SERVEX_IT_ETCD_ENDPOINTS (comma separated) and is skipped when it is
// unset.
func TestIntegrationLoadAndWatch(t *testing.T) {
	endpoints := os.Getenv("SERVEX_IT_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("SERVEX_IT_ETCD_ENDPOINTS not set")
	}
	c, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	prefix := "/servex-it/" + time.Now().Format("150405.000000") + "/"
	t.Cleanup(func() {
		if _, err := c.Delete(context.Background(), prefix, clientv3.WithPrefix()); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	put(t, c, prefix+"http/timeout", `"1s"`)
	put(t, c, prefix+"v", `0`)

	cfg, err := config.New(t.Context(), []config.Source{etcd.New(c, prefix, etcd.WithPrefix())})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Duration("http.timeout"); got != time.Second {
		t.Fatalf("http.timeout = %v", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reloaded := make(chan int, 100)
	done := make(chan error, 1)
	go func() {
		done <- cfg.Watch(ctx, func(c *config.Config) { reloaded <- c.Int("v") })
	}()
	put(t, c, prefix+"http/timeout", `"2s"`)
	put(t, c, prefix+"v", `1`)
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
