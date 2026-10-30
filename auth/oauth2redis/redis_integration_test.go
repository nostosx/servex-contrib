//go:build integration

package oauth2redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/auth/oauth2"
)

func TestIntegrationRedis(t *testing.T) {
	address := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if address == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR not set")
	}
	prefix := "servex-it-oauth-state:" + rand.Text() + ":"
	keys := []string{
		strings.Repeat("a", 64), strings.Repeat("b", 64),
		strings.Repeat("c", 64), strings.Repeat("d", 64),
	}
	var clients [2]*redis.Client
	var stores [2]*Store
	for index := range clients {
		clients[index] = redis.NewClient(&redis.Options{
			Addr: address, MaxRetries: -1, ContextTimeoutEnabled: true,
			DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		names := make([]string, len(keys))
		for index, key := range keys {
			names[index] = prefix + key
		}
		if err := clients[0].Del(ctx, names...).Err(); err != nil {
			t.Error("clean up OAuth state keys:", err)
		}
		for _, client := range clients {
			if err := client.Close(); err != nil {
				t.Error("close Redis client:", err)
			}
		}
	})
	for index, client := range clients {
		if err := client.Ping(t.Context()).Err(); err != nil {
			t.Fatal(err)
		}
		var err error
		stores[index], err = New(client, prefix)
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt := oauth2.Attempt{
		Verifier: "pkce-verifier", Nonce: "oidc-nonce", Binding: sha256.Sum256([]byte("browser-binding")),
		ExpiresAt: time.Now().Add(time.Minute),
	}
	t.Run("insert only and one-time consumption", func(t *testing.T) {
		if err := stores[0].Save(t.Context(), keys[0], attempt); err != nil {
			t.Fatal(err)
		}
		replacement := attempt
		replacement.Nonce = "replacement"
		if err := stores[1].Save(t.Context(), keys[0], replacement); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatalf("duplicate Save = %v, want invalid state", err)
		}
		got, err := stores[1].Take(t.Context(), keys[0])
		if err != nil {
			t.Fatal(err)
		}
		assertIntegrationAttempt(t, got, attempt)
		if _, err := stores[0].Take(t.Context(), keys[0]); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatalf("second Take = %v, want invalid state", err)
		}
	})
	t.Run("concurrent consumers across clients", func(t *testing.T) {
		if err := stores[0].Save(t.Context(), keys[1], attempt); err != nil {
			t.Fatal(err)
		}
		type result struct {
			attempt oauth2.Attempt
			err     error
		}
		const consumers = 24
		start := make(chan struct{})
		results := make(chan result, consumers)
		var group sync.WaitGroup
		for index := range consumers {
			group.Go(func() {
				<-start
				got, err := stores[index%len(stores)].Take(t.Context(), keys[1])
				results <- result{attempt: got, err: err}
			})
		}
		close(start)
		group.Wait()
		close(results)
		winners := 0
		for result := range results {
			if result.err == nil {
				winners++
				assertIntegrationAttempt(t, result.attempt, attempt)
			} else if !errors.Is(result.err, oauth2.ErrInvalidState) {
				t.Errorf("Take returned an unexpected transport error: %v", result.err)
			}
		}
		if winners != 1 {
			t.Fatalf("successful consumers = %d, want 1", winners)
		}
	})
	t.Run("Redis expires login attempt", func(t *testing.T) {
		short := attempt
		short.ExpiresAt = time.Now().Add(500 * time.Millisecond)
		if err := stores[0].Save(t.Context(), keys[2], short); err != nil {
			t.Fatal(err)
		}
		if ttl, err := clients[1].PTTL(t.Context(), prefix+keys[2]).Result(); err != nil || ttl <= 0 || ttl > time.Second {
			t.Fatalf("saved login attempt lacks a bounded Redis expiry: ttl=%s, error=%v", ttl, err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			exists, err := clients[1].Exists(t.Context(), prefix+keys[2]).Result()
			if err != nil {
				t.Fatal(err)
			}
			if exists == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Redis did not expire the login attempt")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := stores[1].Take(t.Context(), keys[2]); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatalf("expired Take = %v, want invalid state", err)
		}
	})
	t.Run("expired payload is rejected and consumed", func(t *testing.T) {
		expired := attempt
		expired.ExpiresAt = time.Now().Add(-time.Second)
		if err := stores[0].Save(t.Context(), keys[3], expired); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatalf("expired Save = %v, want invalid state", err)
		}
		data, err := json.Marshal(expired)
		if err != nil {
			t.Fatal(err)
		}
		if err := clients[0].Set(t.Context(), prefix+keys[3], data, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := stores[1].Take(t.Context(), keys[3]); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatalf("expired payload Take = %v, want invalid state", err)
		}
		if exists, err := clients[0].Exists(t.Context(), prefix+keys[3]).Result(); err != nil || exists != 0 {
			t.Fatalf("expired payload remains after Take: exists=%d, error=%v", exists, err)
		}
	})
}

func assertIntegrationAttempt(t *testing.T, got, want oauth2.Attempt) {
	t.Helper()
	if got.Verifier != want.Verifier || got.Nonce != want.Nonce || got.Binding != want.Binding || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("Take = %+v, want %+v", got, want)
	}
}
