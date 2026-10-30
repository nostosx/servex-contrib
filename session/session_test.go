package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func setup(t *testing.T, opts ...Option) (*Manager, *MemoryStore) {
	t.Helper()
	store, err := NewMemoryStore(100)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(store, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return m, store
}

func call(h http.Handler, path string, cookie *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "https://example.com"+path, nil)
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func responseCookie(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	r := w.Result()
	defer r.Body.Close()
	cookies := r.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %v", cookies)
	}
	return cookies[0]
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func app(t *testing.T, m *Manager) http.Handler {
	t.Helper()
	return m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, ok := FromContext(r.Context())
		if !ok {
			t.Fatal("missing session")
		}
		switch r.URL.Path {
		case "/login":
			must(t, s.RenewToken())
			must(t, s.Put("user", "alice"))
		case "/renew":
			must(t, s.RenewToken())
		case "/logout":
			must(t, s.Destroy())
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var user string
		_, err := s.Get("user", &user)
		must(t, err)
		w.Header().Set("X-User", user)
	}))
}

func TestLifecycle(t *testing.T) {
	m, store := setup(t)
	h := app(t, m)
	first := responseCookie(t, call(h, "/login", nil))
	if !validToken(first.Value) || !first.HttpOnly || !first.Secure || first.SameSite != http.SameSiteLaxMode || first.MaxAge != 0 {
		t.Fatalf("cookie = %+v", first)
	}
	if _, err := store.Load(t.Context(), first.Value); !errors.Is(err, ErrNotFound) {
		t.Fatal("raw token persisted")
	}
	if _, err := store.Load(t.Context(), tokenKey(first.Value)); err != nil {
		t.Fatal(err)
	}
	if call(h, "/me", first).Header().Get("X-User") != "alice" {
		t.Fatal("session lost")
	}
	second := responseCookie(t, call(h, "/renew", first))
	if first.Value == second.Value {
		t.Fatal("token not rotated")
	}
	if call(h, "/me", first).Header().Get("X-User") != "" {
		t.Fatal("old token survived rotation")
	}
	if call(h, "/me", second).Header().Get("X-User") != "alice" {
		t.Fatal("rotation lost values")
	}
	deleted := responseCookie(t, call(h, "/logout", second))
	if deleted.MaxAge != -1 || deleted.Value != "" {
		t.Fatalf("deletion = %+v", deleted)
	}
	if call(h, "/me", second).Header().Get("X-User") != "" {
		t.Fatal("destroyed token survived")
	}
	if len(store.records) != 0 {
		t.Fatal("anonymous read allocated a session")
	}
	if responseCookie(t, call(h, "/me", &http.Cookie{Name: "session", Value: "malformed"})).MaxAge != -1 {
		t.Fatal("invalid cookie not cleared")
	}
}

func TestExpiry(t *testing.T) {
	for _, idle := range []time.Duration{0, 5 * time.Minute} {
		t.Run(idle.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m, _ := setup(t, WithLifetime(15*time.Minute), WithIdleTimeout(idle), WithPersistentCookie())
				h := app(t, m)
				cookie := responseCookie(t, call(h, "/login", nil))
				if cookie.MaxAge <= 0 || cookie.Expires.IsZero() {
					t.Fatal("persistent expiry missing")
				}
				time.Sleep(4 * time.Minute)
				if call(h, "/me", cookie).Header().Get("X-User") != "alice" {
					t.Fatal("early expiry")
				}
				time.Sleep(4 * time.Minute)
				if call(h, "/me", cookie).Header().Get("X-User") != "alice" {
					t.Fatal("idle expiry not refreshed")
				}
				time.Sleep(8 * time.Minute)
				if call(h, "/me", cookie).Header().Get("X-User") != "" {
					t.Fatal("absolute expiry extended")
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		m, _ := setup(t, WithLifetime(time.Hour), WithIdleTimeout(time.Minute))
		h := app(t, m)
		c := responseCookie(t, call(h, "/login", nil))
		time.Sleep(2 * time.Minute)
		if call(h, "/me", c).Header().Get("X-User") != "" {
			t.Fatal("idle expiry ignored")
		}
	})
}

type failingStore struct {
	Store
	loadErr, saveErr error
}

func (s failingStore) Load(ctx context.Context, key string) (Record, error) {
	if s.loadErr != nil {
		return Record{}, s.loadErr
	}
	return s.Store.Load(ctx, key)
}
func (s failingStore) Save(ctx context.Context, key string, version uint64, r Record) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.Store.Save(ctx, key, version, r)
}

func TestFailureResponse(t *testing.T) {
	_, base := setup(t)
	private := errors.New("private backend details")
	m, err := New(failingStore{Store: base, saveErr: private})
	must(t, err)
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, _ := FromContext(r.Context())
		must(t, s.Put("user", "alice"))
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("Content-Encoding", "gzip")
		_, err := io.WriteString(w, "sensitive successful body")
		if !errors.Is(err, private) {
			t.Errorf("write error = %v", err)
		}
		_, _ = io.WriteString(w, "second sensitive body")
	}))
	w := call(h, "/", nil)
	if w.Code != 500 || strings.Contains(w.Body.String(), "sensitive") || strings.Contains(w.Body.String(), "private") || w.Header().Get("Set-Cookie") != "" || w.Header().Get("Content-Encoding") != "" {
		t.Fatalf("failure response = %d %s %v", w.Code, w.Body.String(), w.Header())
	}
	m, err = New(failingStore{Store: base, loadErr: private})
	must(t, err)
	h = m.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("handler called after failed load") }))
	if call(h, "/", &http.Cookie{Name: "session", Value: strings.Repeat("A", 43)}).Code != 500 {
		t.Fatal("load failure accepted")
	}
}

func TestStaleRequestsCannotResurrect(t *testing.T) {
	for _, operation := range []string{"/logout", "/renew"} {
		t.Run(operation, func(t *testing.T) {
			m, store := setup(t)
			cookie := responseCookie(t, call(app(t, m), "/login", nil))
			loaded, release := make(chan struct{}), make(chan struct{})
			done := make(chan *httptest.ResponseRecorder, 1)
			h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s, _ := FromContext(r.Context())
				must(t, s.Put("stale", "value"))
				close(loaded)
				<-release
				_, _ = io.WriteString(w, "must not be sent")
			}))
			go func() { done <- call(h, "/", cookie) }()
			<-loaded
			call(app(t, m), operation, cookie)
			close(release)
			w := <-done
			if w.Code != http.StatusConflict || strings.Contains(w.Body.String(), "must not") {
				t.Fatalf("stale response = %d %s", w.Code, w.Body.String())
			}
			if _, err := store.Load(t.Context(), tokenKey(cookie.Value)); !errors.Is(err, ErrNotFound) {
				t.Fatal("stale snapshot restored revoked key")
			}
		})
	}
}

func TestMemoryStoreAtomicityAndCopies(t *testing.T) {
	s, err := NewMemoryStore(1)
	must(t, err)
	r := Record{Values: map[string]json.RawMessage{"v": json.RawMessage(`"one"`)}, Deadline: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(time.Minute)}
	must(t, s.Save(t.Context(), "old", 0, r))
	r.Values["v"][1] = 'X'
	loaded, err := s.Load(t.Context(), "old")
	must(t, err)
	if string(loaded.Values["v"]) != `"one"` {
		t.Fatal("save aliases caller")
	}
	loaded.Values["v"][1] = 'Y'
	copyRecord, err := s.Load(t.Context(), "old")
	must(t, err)
	if string(copyRecord.Values["v"]) != `"one"` {
		t.Fatal("load aliases store")
	}
	if err := s.Save(t.Context(), "another", 0, copyRecord); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if err := s.Rotate(t.Context(), "old", "new", 2, copyRecord); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err := s.Load(t.Context(), "old"); err != nil {
		t.Fatal("failed rotate deleted old record")
	}
	must(t, s.Rotate(t.Context(), "old", "new", 1, copyRecord))
	if err := s.Save(t.Context(), "old", 1, copyRecord); !errors.Is(err, ErrConflict) {
		t.Fatal("save resurrected rotated key")
	}
	must(t, s.Delete(t.Context(), "new"))
	if err := s.Save(t.Context(), "new", 1, copyRecord); !errors.Is(err, ErrConflict) {
		t.Fatal("save resurrected deleted key")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Save(ctx, "canceled", 0, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, "new"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "new"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Rotate(ctx, "a", "b", 1, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestConcurrentWriters(t *testing.T) {
	_, s := setup(t)
	r := Record{Deadline: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(time.Minute)}
	must(t, s.Save(t.Context(), "key", 0, r))
	var wg sync.WaitGroup
	results := make(chan error, 20)
	for range 20 {
		wg.Go(func() { results <- s.Save(t.Context(), "key", 1, r) })
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("CAS winners = %d", winners)
	}
}

func TestValidationValuesAndCommit(t *testing.T) {
	_, store := setup(t)
	for _, opts := range [][]Option{
		{WithLifetime(0)}, {WithIdleTimeout(-1)}, {WithLifetime(time.Minute)},
		{WithMaxDataBytes(0)}, {WithErrorHandler(nil)}, {WithSameSite(0)},
		{WithCookieName("")}, {WithCookieName("bad;name")},
		{WithInsecureCookies(), WithSameSite(http.SameSiteNoneMode)},
		{WithInsecureCookies(), WithCookieName("__Host-session")},
	} {
		if _, err := New(store, opts...); err == nil {
			t.Fatal("accepted invalid options")
		}
	}
	if _, err := New(nil); err == nil {
		t.Fatal("accepted nil store")
	}
	var nilStore *MemoryStore
	if _, err := New(nilStore); err == nil {
		t.Fatal("accepted typed nil store")
	}
	if _, err := NewMemoryStore(0); err == nil {
		t.Fatal("accepted zero capacity")
	}
	if _, ok := FromContext(nil); ok { //nolint:staticcheck // Verify the explicitly supported nil context.
		t.Fatal("nil context has session")
	}
	m, _ := setup(t, WithMaxDataBytes(30), WithInsecureCookies(), WithSameSite(http.SameSiteStrictMode), WithCookieName("local"), nil)
	var retained *Session
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, _ := FromContext(r.Context())
		retained = s
		must(t, s.Put("v", []string{"one"}))
		var value []string
		_, err := s.Get("v", &value)
		must(t, err)
		value[0] = "changed"
		_, err = s.Get("v", &value)
		must(t, err)
		if value[0] != "one" {
			t.Fatal("Get aliases values")
		}
		if err := s.Put("big", strings.Repeat("x", 40)); !errors.Is(err, ErrDataTooLarge) {
			t.Fatal(err)
		}
		if err := s.Put("bad", make(chan int)); err == nil {
			t.Fatal("invalid JSON accepted")
		}
		if err := s.Put("", 1); err == nil {
			t.Fatal("empty key accepted")
		}
		must(t, s.Remove("v"))
		if found, err := s.Get("v", &value); found || err != nil {
			t.Fatal("Remove failed")
		}
		must(t, s.Put("ok", true))
		must(t, http.NewResponseController(w).Flush())
		if err := s.Put("late", true); !errors.Is(err, ErrCommitted) {
			t.Fatal("mutation after flush accepted")
		}
	}))
	c := responseCookie(t, call(h, "/", nil))
	if c.Secure || c.Name != "local" || c.SameSite != http.SameSiteStrictMode {
		t.Fatal("options not applied")
	}
	if err := retained.RenewToken(); !errors.Is(err, ErrClosed) {
		t.Fatal("request snapshot remained mutable")
	}
	r := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	r.AddCookie(&http.Cookie{Name: "local", Value: strings.Repeat("a", 43)})
	r.AddCookie(&http.Cookie{Name: "local", Value: strings.Repeat("b", 43)})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatal("ambiguous cookies accepted")
	}
}

func TestTLSCookieRoundtrip(t *testing.T) {
	m, _ := setup(t)
	server := httptest.NewTLSServer(app(t, m))
	defer server.Close()
	client := server.Client()
	jar, err := cookiejar.New(nil)
	must(t, err)
	client.Jar = jar
	for _, path := range []string{"/login", "/me", "/logout", "/me"} {
		r, err := client.Post(server.URL+path, "text/plain", nil)
		must(t, err)
		if (path == "/login" || path == "/me") && r.StatusCode != http.StatusOK {
			t.Fatal(r.StatusCode)
		}
		if path == "/me" && len(jar.Cookies(r.Request.URL)) > 0 && r.Header.Get("X-User") != "alice" {
			t.Fatal("cookie did not roundtrip")
		}
		must(t, r.Body.Close())
	}
}

func TestPanicDoesNotCommit(t *testing.T) {
	m, s := setup(t)
	h := m.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		current, _ := FromContext(r.Context())
		must(t, current.Put("user", "alice"))
		panic("abort")
	}))
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic swallowed")
			}
		}()
		call(h, "/", nil)
	}()
	if len(s.records) != 0 {
		t.Fatal("aborted handler committed")
	}
}

func TestExpiredCapacityReclaimed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := NewMemoryStore(1)
		must(t, err)
		record := Record{Deadline: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(time.Minute)}
		must(t, store.Save(t.Context(), "first", 0, record))
		time.Sleep(2 * time.Minute)
		record.ExpiresAt = time.Now().Add(time.Minute)
		must(t, store.Save(t.Context(), "second", 0, record))
		if _, err := store.Load(t.Context(), "first"); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	})
}

func TestReadOnlyRefreshCannotResurrect(t *testing.T) {
	m, store := setup(t)
	cookie := responseCookie(t, call(app(t, m), "/login", nil))
	loaded, release := make(chan struct{}), make(chan struct{})
	done := make(chan *httptest.ResponseRecorder, 1)
	h := m.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { close(loaded); <-release }))
	go func() { done <- call(h, "/", cookie) }()
	<-loaded
	call(app(t, m), "/logout", cookie)
	close(release)
	w := <-done
	if w.Code != http.StatusOK || w.Header().Get("Set-Cookie") != "" {
		t.Fatalf("refresh response=%d %v", w.Code, w.Header())
	}
	if _, err := store.Load(t.Context(), tokenKey(cookie.Value)); !errors.Is(err, ErrNotFound) {
		t.Fatal("idle refresh restored revoked key")
	}
}

func TestResponseCapabilities(t *testing.T) {
	m, _ := setup(t)
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current, _ := FromContext(r.Context())
		must(t, current.Put("user", "alice"))
		if _, _, err := http.NewResponseController(w).Hijack(); !errors.Is(err, ErrPendingChanges) {
			t.Fatalf("pending hijack=%v", err)
		}
		must(t, http.NewResponseController(w).Flush())
		if err := current.Remove("user"); !errors.Is(err, ErrCommitted) {
			t.Fatal("post-flush mutation allowed")
		}
	}))
	w := call(h, "/", nil)
	if !w.Flushed || responseCookie(t, w).Value == "" {
		t.Fatal("flush lost cookie")
	}
	h = m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, _, err := http.NewResponseController(w).Hijack(); !errors.Is(err, http.ErrNotSupported) {
			t.Fatalf("unsupported hijack=%v", err)
		}
		w.WriteHeader(http.StatusNotImplemented)
	}))
	if w := call(h, "/", nil); w.Code != http.StatusNotImplemented {
		t.Fatalf("hijack error committed response: %d", w.Code)
	}
}
