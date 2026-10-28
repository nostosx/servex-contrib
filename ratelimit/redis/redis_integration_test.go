//go:build integration

package redis_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redislimit "github.com/nostosx/servex-contrib/ratelimit/redis"
)

func TestIntegrationSharedLimit(t *testing.T) {
	addr := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR is not configured")
	}
	prefix := "servex-it-limit:" + t.Name() + ":" + time.Now().Format("150405.000000000") + ":"
	var clients []*goredis.Client
	var limits []*redislimit.Limiter
	for range 2 {
		c := goredis.NewClient(&goredis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = c.Close() })
		clients = append(clients, c)
		l, err := redislimit.New(c, 0.001, 10, redislimit.WithPrefix(prefix))
		if err != nil {
			t.Fatal(err)
		}
		limits = append(limits, l)
	}
	t.Cleanup(func() {
		keys, err := clients[0].Keys(t.Context(), prefix+"*").Result()
		if err == nil && len(keys) != 0 {
			_ = clients[0].Del(t.Context(), keys...).Err()
		}
	})
	var count atomic.Int64
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Go(func() {
			allowed, delay, err := limits[i%len(limits)].AllowContext(t.Context(), "shared")
			switch {
			case err != nil:
				t.Error(err)
			case allowed:
				count.Add(1)
			case delay <= 0:
				t.Error("rejection has no retry hint")
			}
		})
	}
	wg.Wait()
	if count.Load() != 10 {
		t.Fatalf("two replicas granted %d, want 10", count.Load())
	}
}
