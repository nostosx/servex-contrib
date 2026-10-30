//go:build integration

package redis

import (
	"errors"
	"os"
	"testing"
	"time"
	"uuid"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/session"
)

func TestIntegrationRedis(t *testing.T) {
	address := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if address == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR not set")
	}
	client := goredis.NewClient(&goredis.Options{Addr: address, MaxRetries: -1, ContextTimeoutEnabled: true, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	t.Cleanup(func() { must(t, client.Close()) })
	must(t, client.Ping(t.Context()).Err())
	s, err := New(client, WithPrefix("servex-it-session:"+uuid.NewV7().String()+":"))
	must(t, err)
	exercise(t, s, client)
	exerciseManagers(t, s, client)
	key := hashed("real-expiry")
	r := record()
	r.ExpiresAt = time.Now().Add(150 * time.Millisecond)
	must(t, s.Save(t.Context(), key, 0, r))
	deadline := time.Now().Add(3 * time.Second)
	for {
		exists, err := client.Exists(t.Context(), s.options.prefix+key).Result()
		must(t, err)
		if exists == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Redis did not expire the key")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.Save(t.Context(), key, 1, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("expired Redis record resurrected")
	}
}
