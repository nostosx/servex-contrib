//go:build integration

package redis

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	cacheloader "github.com/nostosx/servex-contrib/cache/loader"
	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/storage/cache"
)

// integrationClient connects to the Redis server at SERVEX_IT_REDIS_ADDR,
// skipping the test when it is unset.
func integrationClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR not set")
	}
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{addr}})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return client
}

func TestIntegrationCache(t *testing.T) {
	client := integrationClient(t)
	prefix := "servex-it:" + time.Now().Format("150405.000000000") + ":"
	c := New(client, WithPrefix(prefix))
	ctx := t.Context()
	t.Cleanup(func() {
		_ = client.Del(context.WithoutCancel(ctx), prefix+"k", prefix+"ttl", prefix+"loader").Err()
	})

	if _, err := c.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get missing = %v", err)
	}
	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if ttl := client.TTL(ctx, prefix+"k").Val(); ttl != -1 {
		t.Fatalf("TTL of ttl<=0 entry = %v, want none (-1)", ttl)
	}
	if got, err := c.Get(ctx, "k"); err != nil || string(got) != "v" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after Delete = %v", err)
	}

	if err := c.Set(ctx, "ttl", []byte("v"), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := c.Get(ctx, "ttl")
		if errors.Is(err, cache.ErrNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("entry did not expire")
		}
		time.Sleep(20 * time.Millisecond)
	}

	l, err := cacheloader.New[int](t.Context(), c, time.Minute, cacheloader.WithCodec(jsoncodec.Codec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	got, err := l.Get(ctx, "loader", func(context.Context) (int, error) { return 42, nil })
	if err != nil || got != 42 {
		t.Fatalf("Loader.Get = %d, %v", got, err)
	}
	if raw, err := client.Get(ctx, prefix+"loader").Result(); err != nil || raw != "42" {
		t.Fatalf("written back %q, %v", raw, err)
	}
}
