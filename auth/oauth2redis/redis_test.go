package oauth2redis

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/auth/oauth2"
)

func TestAtomicSharedConsumption(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	first, _ := New(client, "app:oauth:")
	second, _ := New(client, "app:oauth:")
	key := strings.Repeat("a", 64)
	attempt := oauth2.Attempt{Verifier: "pkce", Nonce: "nonce", ExpiresAt: time.Now().Add(time.Minute)}
	if err := first.Save(t.Context(), key, attempt); err != nil {
		t.Fatal(err)
	}
	if err := first.Save(t.Context(), key, attempt); !errors.Is(err, oauth2.ErrInvalidState) {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if got, err := second.Take(t.Context(), key); err == nil {
				if got.Verifier != "pkce" {
					t.Error(got)
				}
				winners.Add(1)
			}
		})
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal(winners.Load())
	}
	if _, err := first.Take(t.Context(), key); !errors.Is(err, oauth2.ErrInvalidState) {
		t.Fatal(err)
	}
	if err := first.Save(t.Context(), key, attempt); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Minute)
	if _, err := second.Take(t.Context(), key); !errors.Is(err, oauth2.ErrInvalidState) {
		t.Fatal(err)
	}
}
