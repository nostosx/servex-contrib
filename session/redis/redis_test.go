package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/session"
)

func hashed(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func record() session.Record {
	return session.Record{Values: map[string]json.RawMessage{"id": json.RawMessage(`9007199254740993`)}, Deadline: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(time.Minute)}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func fixture(t *testing.T) (*Store, *goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() {
		if err := client.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			t.Error(err)
		}
	})
	s, err := New(client, WithPrefix("session-test:"))
	must(t, err)
	return s, client, server
}

// exercise runs the same contract checks on the local fixture and real Redis.
func exercise(t *testing.T, s *Store, client *goredis.Client) {
	t.Helper()
	ctx := t.Context()
	key, newKey, collision := hashed("old"), hashed("new"), hashed("collision")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Del(ctx, s.options.prefix+key, s.options.prefix+newKey, s.options.prefix+collision).Err()
	})
	if _, err := s.Load(ctx, key); !errors.Is(err, session.ErrNotFound) {
		t.Fatal(err)
	}
	r := record()
	must(t, s.Save(ctx, key, 0, r))
	r.Values["id"][0] = '1'
	loaded, err := s.Load(ctx, key)
	must(t, err)
	if loaded.Version != 1 || string(loaded.Values["id"]) != "9007199254740993" {
		t.Fatalf("loaded=%+v", loaded)
	}
	ttl, err := client.PTTL(ctx, s.options.prefix+key).Result()
	must(t, err)
	if ttl <= 0 {
		t.Fatalf("TTL=%v", ttl)
	}
	// Container and client clocks may differ; compare the stored absolute
	// deadline rather than assuming a shared wall clock for relative TTLs.
	expiresAt, err := client.PExpireTime(ctx, s.options.prefix+key).Result()
	must(t, err)
	if expiresAt.Milliseconds() != r.ExpiresAt.UnixMilli() {
		t.Fatalf("expiry=%v, want %v", expiresAt.Milliseconds(), r.ExpiresAt.UnixMilli())
	}
	if err := s.Save(ctx, key, 0, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("duplicate create accepted")
	}
	must(t, s.Save(ctx, key, 1, loaded))
	if err := s.Save(ctx, key, 1, loaded); !errors.Is(err, session.ErrConflict) {
		t.Fatal("stale update accepted")
	}
	must(t, s.Save(ctx, collision, 0, record()))
	if err := s.Rotate(ctx, key, collision, 2, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("rotation collision accepted")
	}
	if _, err := s.Load(ctx, key); err != nil {
		t.Fatal("failed rotation removed old key")
	}
	must(t, s.Rotate(ctx, key, newKey, 2, record()))
	if _, err := s.Load(ctx, key); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("old key survived")
	}
	if err := s.Save(ctx, key, 2, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("revoked key resurrected")
	}
	loaded, err = s.Load(ctx, newKey)
	must(t, err)
	if loaded.Version != 1 {
		t.Fatal(loaded.Version)
	}
	must(t, s.Delete(ctx, newKey))
	must(t, s.Delete(ctx, newKey))
	if err := s.Save(ctx, newKey, 1, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("deleted key resurrected")
	}
	if err := s.Rotate(ctx, newKey, key, 1, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("deleted key rotated")
	}
}

func TestStoreContract(t *testing.T) { s, c, _ := fixture(t); exercise(t, s, c) }

func TestExpiryAndScriptReload(t *testing.T) {
	s, c, server := fixture(t)
	key := hashed("expiry")
	must(t, s.Save(t.Context(), key, 0, record()))
	must(t, c.ScriptFlush(t.Context()).Err())
	must(t, s.Save(t.Context(), key, 1, record()))
	server.FastForward(2 * time.Minute)
	if _, err := s.Load(t.Context(), key); !errors.Is(err, session.ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.Save(t.Context(), key, 2, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal("expired key resurrected")
	}
	r := record()
	r.ExpiresAt = time.Now().Add(-time.Second)
	if err := s.Save(t.Context(), key, 0, r); !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
}

func TestExactVersionAndPayload(t *testing.T) {
	s, c, _ := fixture(t)
	key := hashed("precision")
	r := record()
	r.Version = 9007199254740993
	data, err := json.Marshal(r)
	must(t, err)
	must(t, c.HSet(t.Context(), s.options.prefix+key, "version", strconv.FormatUint(r.Version, 10), "record", data).Err())
	must(t, c.PExpireAt(t.Context(), s.options.prefix+key, r.ExpiresAt).Err())
	must(t, s.Save(t.Context(), key, r.Version, r))
	got, err := s.Load(t.Context(), key)
	must(t, err)
	if got.Version != r.Version+1 || string(got.Values["id"]) != "9007199254740993" {
		t.Fatal("numeric precision lost")
	}
	if err := s.Save(t.Context(), key, math.MaxUint64, r); !errors.Is(err, session.ErrConflict) {
		t.Fatal("version overflow accepted")
	}
}

func TestConcurrentCAS(t *testing.T) {
	s, _, _ := fixture(t)
	key := hashed("concurrent")
	must(t, s.Save(t.Context(), key, 0, record()))
	results := make(chan error, 20)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { results <- s.Save(t.Context(), key, 1, record()) })
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, session.ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("CAS winners=%d", winners)
	}
}

func TestValidationAndBackendFailures(t *testing.T) {
	s, c, _ := fixture(t)
	if _, err := New(nil); err == nil {
		t.Fatal("nil client accepted")
	}
	for _, opts := range [][]Option{{WithPrefix("")}, {WithMaxRecordBytes(0)}} {
		if _, err := New(c, opts...); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	if _, err := s.Load(t.Context(), "raw-token"); err == nil {
		t.Fatal("raw token accepted")
	}
	key := hashed("corrupt")
	must(t, c.HSet(t.Context(), s.options.prefix+key, "version", "1", "record", "bad-json").Err())
	if _, err := s.Load(t.Context(), key); !errors.Is(err, ErrInvalidRecord) {
		t.Fatal(err)
	}
	small, err := New(c, WithMaxRecordBytes(5))
	must(t, err)
	if err := small.Save(t.Context(), hashed("large"), 0, record()); !errors.Is(err, ErrInvalidRecord) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	want := errors.New("request ended")
	cancel(want)
	if _, err := s.Load(ctx, key); !errors.Is(err, want) {
		t.Fatal(err)
	}
	for _, err := range []error{s.Save(ctx, key, 0, record()), s.Delete(ctx, key), s.Rotate(ctx, key, hashed("other"), 1, record())} {
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
	must(t, c.Close())
	if _, err := s.Load(t.Context(), key); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("lost client error: %v", err)
	}
}

func TestSharedManagers(t *testing.T) {
	s, client, _ := fixture(t)
	exerciseManagers(t, s, client)
}

func exerciseManagers(t *testing.T, s *Store, client *goredis.Client) {
	t.Helper()
	other, err := New(client, WithPrefix(s.options.prefix))
	must(t, err)
	first, err := session.New(s)
	must(t, err)
	second, err := session.New(other)
	must(t, err)
	handler := func(manager *session.Manager) http.Handler {
		return manager.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			current, _ := session.FromContext(r.Context())
			if r.URL.Path == "/login" {
				must(t, current.RenewToken())
				must(t, current.Put("user", "alice"))
			}
			if r.URL.Path == "/logout" {
				must(t, current.Destroy())
				return
			}
			var user string
			_, err := current.Get("user", &user)
			must(t, err)
			w.Header().Set("X-User", user)
		}))
	}
	r := httptest.NewRequest(http.MethodPost, "https://example.com/login", nil)
	w := httptest.NewRecorder()
	handler(first).ServeHTTP(w, r)
	response := w.Result()
	defer response.Body.Close()
	cookies := response.Cookies()
	if len(cookies) != 1 {
		t.Fatal(cookies)
	}
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "https://example.com"+path, nil)
		r.AddCookie(cookies[0])
		w := httptest.NewRecorder()
		handler(second).ServeHTTP(w, r)
		return w
	}
	if request("/me").Header().Get("X-User") != "alice" {
		t.Fatal("managers do not share session")
	}
	request("/logout")
	if request("/me").Header().Get("X-User") != "" {
		t.Fatal("logout not shared")
	}
	keys, err := client.Keys(t.Context(), s.options.prefix+"*").Result()
	must(t, err)
	for _, key := range keys {
		if strings.Contains(key, cookies[0].Value) {
			t.Fatal("raw token in Redis")
		}
	}
}

func TestWrongTypeRotationPreservesOld(t *testing.T) {
	s, c, _ := fixture(t)
	old, newKey := hashed("old"), hashed("wrong-type")
	must(t, s.Save(t.Context(), old, 0, record()))
	must(t, c.Set(t.Context(), s.options.prefix+newKey, "occupied", 0).Err())
	if err := s.Rotate(t.Context(), old, newKey, 1, record()); !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
	if _, err := s.Load(t.Context(), old); err != nil {
		t.Fatal(err)
	}
}
