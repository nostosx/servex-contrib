package oauth2redis

import (
	"context"
	"encoding/json"
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

func TestStateStoreRejectsInvalidConfiguration(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	for _, tt := range []struct {
		name   string
		client *redis.Client
		prefix string
	}{
		{name: "nil client", prefix: "app:"},
		{name: "empty prefix", client: client},
		{name: "oversized prefix", client: client, prefix: strings.Repeat("p", 257)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if store, err := New(tt.client, tt.prefix); err == nil || store != nil {
				t.Fatalf("invalid configuration accepted: store=%v err=%v", store, err)
			}
		})
	}
}

func TestStateStoreRejectsInvalidInputAndPreservesCanceledState(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	store, err := New(client, "app:oauth:")
	if err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 64)
	live := oauth2.Attempt{Verifier: "pkce", Nonce: "nonce", ExpiresAt: time.Now().Add(time.Minute)}
	for _, tt := range []struct {
		name    string
		key     string
		attempt oauth2.Attempt
	}{
		{name: "short key", key: "bad", attempt: live},
		{name: "nonhex key", key: strings.Repeat("z", 64), attempt: live},
		{name: "expired attempt", key: key, attempt: oauth2.Attempt{ExpiresAt: time.Now().Add(-time.Second)}},
		{name: "oversized attempt", key: key, attempt: oauth2.Attempt{Verifier: strings.Repeat("v", 4096), ExpiresAt: live.ExpiresAt}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := store.Save(t.Context(), tt.key, tt.attempt); !errors.Is(err, oauth2.ErrInvalidState) {
				t.Fatal("invalid attempt accepted", err)
			}
			if len(server.Keys()) != 0 {
				t.Fatal("invalid attempt was persisted")
			}
		})
	}
	for _, key := range []string{"bad", strings.Repeat("z", 64)} {
		if _, err := store.Take(t.Context(), key); !errors.Is(err, oauth2.ErrInvalidState) {
			t.Fatal("invalid key accepted", err)
		}
	}
	if err := store.Save(t.Context(), key, live); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Save(ctx, strings.Repeat("b", 64), live); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Save lost its cause", err)
	}
	if _, err := store.Take(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Take lost its cause", err)
	}
	if len(server.Keys()) != 1 {
		t.Fatal("canceled operations changed stored state", server.Keys())
	}
	if got, err := store.Take(t.Context(), key); err != nil || got.Verifier != live.Verifier || got.Nonce != live.Nonce || got.Binding != live.Binding || !got.ExpiresAt.Equal(live.ExpiresAt) {
		t.Fatalf("canceled Take consumed the live attempt: got=%+v err=%v", got, err)
	}
}

func TestStateStoreConsumesInvalidRecordsAndPreservesBackendErrors(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	store, err := New(client, "app:oauth:")
	if err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 64)
	expired, err := json.Marshal(oauth2.Attempt{ExpiresAt: time.Now().Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		data string
	}{
		{name: "malformed JSON", data: "{"},
		{name: "oversized record", data: strings.Repeat("x", 4097)},
		{name: "expired record", data: string(expired)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := server.Set("app:oauth:"+key, tt.data); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Take(t.Context(), key); !errors.Is(err, oauth2.ErrInvalidState) {
				t.Fatal("invalid record accepted", err)
			}
			if server.Exists("app:oauth:" + key) {
				t.Fatal("invalid record remained replayable")
			}
		})
	}
	const want = "ERR state backend unavailable"
	server.SetError(want)
	live := oauth2.Attempt{ExpiresAt: time.Now().Add(time.Minute)}
	if err := store.Save(t.Context(), key, live); err == nil || err.Error() != want {
		t.Fatal("Save replaced backend error", err)
	} else if _, ok := errors.AsType[redis.Error](err); !ok {
		t.Fatal("Save erased the backend error type", err)
	}
	if _, err := store.Take(t.Context(), key); err == nil || err.Error() != want {
		t.Fatal("Take replaced backend error", err)
	} else if _, ok := errors.AsType[redis.Error](err); !ok {
		t.Fatal("Take erased the backend error type", err)
	}
}
