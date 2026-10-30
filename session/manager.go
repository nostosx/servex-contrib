package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"reflect"
	"strings"
	"time"
)

// ErrInvalidCookie identifies ambiguous duplicate session cookies.
var ErrInvalidCookie = errors.New("session: ambiguous cookies")

// Option configures an immutable Manager.
type Option func(*options)

type options struct {
	cookie       http.Cookie
	lifetime     time.Duration
	idleTimeout  time.Duration
	maxDataBytes int
	persistent   bool
	onError      func(http.ResponseWriter, *http.Request, error)
}

func WithCookieName(name string) Option { return func(o *options) { o.cookie.Name = name } }
func WithLifetime(lifetime time.Duration) Option {
	return func(o *options) { o.lifetime = lifetime }
}

// WithIdleTimeout sets inactivity expiry. Zero disables it.
func WithIdleTimeout(timeout time.Duration) Option {
	return func(o *options) { o.idleTimeout = timeout }
}

func WithSameSite(mode http.SameSite) Option {
	return func(o *options) { o.cookie.SameSite = mode }
}

// WithInsecureCookies permits HTTP cookies for local development.
func WithInsecureCookies() Option      { return func(o *options) { o.cookie.Secure = false } }
func WithPersistentCookie() Option     { return func(o *options) { o.persistent = true } }
func WithMaxDataBytes(size int) Option { return func(o *options) { o.maxDataBytes = size } }

// WithErrorHandler sets an application-owned error response/logging callback.
// It must write an error response and must not expose raw backend errors.
func WithErrorHandler(handler func(http.ResponseWriter, *http.Request, error)) Option {
	return func(o *options) { o.onError = handler }
}

// Manager implements cookie loading and persistence. It owns no goroutines and
// does not close its injected Store. Reuse it across requests.
type Manager struct {
	store   Store
	options options
}

// New requires an explicit Store. Defaults: host-only HttpOnly/Secure/Lax cookie
// named session, path /, 24-hour absolute and 30-minute idle expiry, 64 KiB JSON
// data limit, browser-session cookie. Only hashed random tokens reach Store.
func New(store Store, opts ...Option) (*Manager, error) {
	if store == nil || nilValue(store) {
		return nil, errors.New("session: store is required")
	}
	o := options{
		cookie:   http.Cookie{Name: "session", Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode},
		lifetime: 24 * time.Hour, idleTimeout: 30 * time.Minute, maxDataBytes: 64 << 10,
		onError: defaultErrorHandler,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.lifetime <= 0 || o.idleTimeout < 0 || o.idleTimeout > o.lifetime || o.maxDataBytes <= 0 || o.onError == nil {
		return nil, errors.New("session: invalid lifetime, idle timeout, data limit or error handler")
	}
	if o.cookie.SameSite != http.SameSiteLaxMode && o.cookie.SameSite != http.SameSiteStrictMode && o.cookie.SameSite != http.SameSiteNoneMode {
		return nil, errors.New("session: SameSite must be Lax, Strict or None")
	}
	if !o.cookie.Secure && (o.cookie.SameSite == http.SameSiteNoneMode || strings.HasPrefix(o.cookie.Name, "__Host-") || strings.HasPrefix(o.cookie.Name, "__Secure-")) {
		return nil, errors.New("session: cookie policy requires Secure")
	}
	if err := o.cookie.Valid(); err != nil {
		return nil, err
	}
	return &Manager{store: store, options: o}, nil
}

// Middleware loads a request snapshot and commits before the first response
// write/flush, or on normal handler return. Failed persistence suppresses the
// handler's response body. Session changes must precede writes; no commit occurs
// after a handler panic. Use httpx.WithHTTPMiddleware to cover native handlers.
func (m *Manager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if current, ok := FromContext(r.Context()); ok && current.manager == m {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Cookie")
		w.Header().Set("Cache-Control", "private, no-store")
		s, err := m.load(r)
		if err != nil {
			m.options.onError(w, r, err)
			return
		}
		defer func() {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
		}()
		r = r.WithContext(context.WithValue(r.Context(), contextKey{}, s))
		writer := &responseWriter{ResponseWriter: w, request: r, session: s}
		next.ServeHTTP(writer, r)
		if !writer.written {
			_ = writer.ensure()
		}
	})
}

func (m *Manager) load(r *http.Request) (*Session, error) {
	if err := context.Cause(r.Context()); err != nil {
		return nil, err
	}
	now := time.Now()
	s := &Session{manager: m, record: Record{Values: make(map[string]json.RawMessage), Deadline: now.Add(m.options.lifetime)}}
	var token string
	count := 0
	for _, cookie := range r.Cookies() {
		if cookie.Name == m.options.cookie.Name {
			token = cookie.Value
			count++
		}
	}
	if count > 1 {
		return nil, ErrInvalidCookie
	}
	if count == 0 {
		return s, nil
	}
	s.clearCookie = true
	if !validToken(token) {
		return s, nil
	}
	key := tokenKey(token)
	record, err := m.store.Load(r.Context(), key)
	if errors.Is(err, ErrNotFound) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err := context.Cause(r.Context()); err != nil {
		return nil, err
	}
	if expired(record, time.Now()) {
		return s, nil
	}
	if record.Version == 0 || record.ExpiresAt.After(record.Deadline) {
		return nil, errors.New("session: invalid stored record")
	}
	data, err := json.Marshal(record.Values)
	if err != nil {
		return nil, err
	}
	if len(data) > m.options.maxDataBytes {
		return nil, ErrDataTooLarge
	}
	s.record, s.key, s.clearCookie = cloneRecord(record), key, false
	return s, nil
}

func (m *Manager) commit(ctx context.Context, w http.ResponseWriter, s *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed = true
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if s.destroyed {
		if s.key != "" {
			if err := m.store.Delete(ctx, s.key); err != nil {
				return err
			}
		}
		m.writeCookie(w, "", time.Time{})
		return nil
	}
	changed := s.dirty || s.rotate
	if !changed && s.key == "" {
		if s.clearCookie {
			m.writeCookie(w, "", time.Time{})
		}
		return nil
	}
	if !changed && m.options.idleTimeout == 0 {
		return nil
	}
	record := cloneRecord(s.record)
	record.ExpiresAt = record.Deadline
	if m.options.idleTimeout > 0 {
		if idle := time.Now().Add(m.options.idleTimeout); idle.Before(record.Deadline) {
			record.ExpiresAt = idle
		}
	}
	if s.key == "" || s.rotate {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return err
		}
		token := base64.RawURLEncoding.EncodeToString(entropy[:])
		key := tokenKey(token)
		var err error
		if s.key == "" {
			err = m.store.Save(ctx, key, 0, record)
		} else {
			err = m.store.Rotate(ctx, s.key, key, s.record.Version, record)
		}
		if err != nil {
			return err
		}
		m.writeCookie(w, token, record.Deadline)
		return nil
	}
	err := m.store.Save(ctx, s.key, s.record.Version, record)
	if !changed && errors.Is(err, ErrConflict) {
		// Read-only idle refresh is best effort: concurrent saves/deletions win.
		// It never creates a key and never reissues the old cookie.
		return nil
	}
	return err
}

func (m *Manager) writeCookie(w http.ResponseWriter, token string, deadline time.Time) {
	cookie := m.options.cookie
	cookie.Value = token
	if deadline.IsZero() {
		cookie.Expires, cookie.MaxAge = time.Unix(1, 0), -1
	} else if m.options.persistent {
		cookie.Expires = deadline
		cookie.MaxAge = int(math.Ceil(time.Until(deadline).Seconds()))
	}
	http.SetCookie(w, &cookie)
}

func tokenKey(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func validToken(token string) bool {
	if len(token) != 43 {
		return false
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(token)
	return err == nil && len(data) == 32
}

func nilValue(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

func defaultErrorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, ErrCapacity):
		code = http.StatusServiceUnavailable
	case errors.Is(err, ErrInvalidCookie):
		code = http.StatusBadRequest
	}
	http.Error(w, http.StatusText(code), code)
}
