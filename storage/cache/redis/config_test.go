package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"pool_size":7,"min_idle_conns":0,"conn_max_idle_time":0,"max_retries":0}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	native := cfg.clientOptions()
	if native.PoolSize != 7 || native.MinIdleConns != 0 || native.ConnMaxIdleTime != -1 || native.MaxRetries != -1 || !native.ContextTimeoutEnabled {
		t.Fatalf("explicit zero values were lost: %#v", native)
	}
	cfg.Addrs[0] = "changed:6379"
	if native.Addrs[0] != "127.0.0.1:6379" {
		t.Fatal("native configuration aliases caller addresses")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Addrs = nil },
		func(c *Config) { c.Addrs = []string{"secret-password"} },
		func(c *Config) { c.Addrs = []string{"localhost:0"} },
		func(c *Config) { c.Addrs = []string{"localhost:+6379"} },
		func(c *Config) { c.Addrs = []string{"localhost:\u20036379"} },
		func(c *Config) { c.Addrs = []string{"localhost:6379", "localhost:6380"}; c.DB = 1 },
		func(c *Config) { c.SentinelPassword = "secret-password" },
		func(c *Config) { c.DB = -1 },
		func(c *Config) { c.MasterName = " master " },
		func(c *Config) { c.ReadTimeout = 0 },
		func(c *Config) { c.DialTimeout = -time.Second },
		func(c *Config) { c.PoolSize = 0 },
		func(c *Config) { limit := int64(math.MaxInt32); c.PoolSize = int(limit + 1) },
		func(c *Config) { c.MinIdleConns = c.PoolSize + 1 },
		func(c *Config) { c.ConnMaxLifetime = -1 },
		func(c *Config) { c.MaxRetries = -1 },
		func(c *Config) { c.MaxRedirects = -1 },
		func(c *Config) { c.ServerName = "secret-password" },
	} {
		invalid := DefaultConfig()
		change(&invalid)
		result, err := Open(invalid)
		if result != nil || !errors.Is(err, ErrInvalidConfig) || strings.Contains(err.Error(), "secret-password") {
			t.Fatalf("invalid configuration = %v, %v", result, err)
		}
	}
}

func TestOpenOwnedLifecycleAndBorrowedClient(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := DefaultConfig()
	cfg.Addrs = []string{server.Addr()}
	cfg.Prefix = "configured:"
	owned, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owned.Close() })
	hook := Hook("redis", owned)
	checker, ok := hook.(health.Checker)
	if !ok || hook.Name() != "redis" {
		t.Fatal("hook does not expose readiness")
	}
	if err := hook.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := owned.Set(t.Context(), "entry", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if value, err := server.Get("configured:entry"); err != nil || value != "value" {
		t.Fatalf("configured prefix = %q, %v", value, err)
	}
	borrowed := New(owned.Client(), WithPrefix("borrowed:"))
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := checker.Check(t.Context()); err != nil {
		t.Fatalf("borrower closed owner: %v", err)
	}
	if err := hook.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := borrowed.Get(t.Context(), "entry"); !errors.Is(err, goredis.ErrClosed) || errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatalf("closed owned pool = %v", err)
	}
}

func TestOpenTLSAndTopology(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS, cfg.ServerName = true, "redis.example"
	cache, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	native := cache.Client().(*goredis.Client).Options()
	if native.TLSConfig == nil || native.TLSConfig.MinVersion != tls.VersionTLS12 || native.TLSConfig.ServerName != "redis.example" {
		t.Fatal("file TLS policy was not applied")
	}
	runtimeTLS := &tls.Config{MinVersion: tls.VersionTLS13}
	runtimeCache, err := Open(cfg, WithTLSConfig(runtimeTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeCache.Close()
	runtimeTLS.MinVersion = tls.VersionTLS12
	if runtimeCache.Client().(*goredis.Client).Options().TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Fatal("runtime TLS configuration was not copied")
	}
	cfg.TLS, cfg.ServerName = false, ""
	cfg.Addrs = []string{"127.0.0.1:16379", "127.0.0.1:16380"}
	cfg.MaxRetries, cfg.MaxRedirects = 0, 0
	cluster, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	clusterClient, ok := cluster.Client().(*goredis.ClusterClient)
	if !ok {
		t.Fatal("multiple seeds did not select Cluster")
	}
	if clusterClient.Options().MaxRetries != -1 || clusterClient.Options().MaxRedirects != 0 {
		t.Fatal("explicit zero Cluster retry budgets were lost")
	}
	cfg.MasterName = "master"
	cfg.DB = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Sentinel DB selection rejected: %v", err)
	}
}

type blockingCloseClient struct {
	goredis.UniversalClient
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
	err     error
}

func (c *blockingCloseClient) Close() error {
	c.calls.Add(1)
	close(c.started)
	<-c.release
	return c.err
}

func TestHookStopCancellationJoinsOwnedCleanup(t *testing.T) {
	want := errors.New("close failure")
	interrupted := errors.New("caller stopped waiting")
	client := &blockingCloseClient{started: make(chan struct{}), release: make(chan struct{}), err: want}
	cache := &Cache{client: client, owner: &clientOwner{client: client, done: make(chan struct{})}}
	hook := Hook("owned", cache)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(interrupted)
	if err := hook.Stop(ctx); !errors.Is(err, interrupted) {
		t.Fatalf("canceled Stop = %v", err)
	}
	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		close(client.release)
		t.Fatal("cleanup did not start")
	}
	close(client.release)
	if err := hook.Stop(t.Context()); !errors.Is(err, want) {
		t.Fatalf("joined Stop = %v", err)
	}
	if err := cache.Close(); !errors.Is(err, want) || client.calls.Load() != 1 {
		t.Fatalf("Close = %v, calls = %d", err, client.calls.Load())
	}
}

type failedPingClient struct {
	goredis.UniversalClient
	failure error
	closed  atomic.Int32
}

func (c *failedPingClient) Ping(ctx context.Context) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, "ping")
	cmd.SetErr(c.failure)
	return cmd
}
func (c *failedPingClient) Close() error { c.closed.Add(1); return nil }

func TestHookStartupFailureCleansOnlyOwnedClient(t *testing.T) {
	failure := errors.New("ping failed")
	for _, owned := range []bool{false, true} {
		client := &failedPingClient{failure: failure}
		cache := New(client)
		if owned {
			cache.owner = &clientOwner{client: client, done: make(chan struct{})}
		}
		if err := Hook("redis", cache).Start(t.Context()); !errors.Is(err, failure) {
			t.Fatalf("startup lost failure: %v", err)
		}
		want := int32(0)
		if owned {
			want = 1
		}
		if got := client.closed.Load(); got != want {
			t.Fatalf("owned=%v: closed=%d, want %d", owned, got, want)
		}
		if err := cache.Close(); err != nil || client.closed.Load() != want {
			t.Fatalf("later Close changed ownership: %v", err)
		}
	}
}
