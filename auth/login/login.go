// Package login composes OAuth2/OIDC and browser sessions around an application
// principal. Password, JWT and browser handlers share endpoint.PrincipalFromContext[P].
package login

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	sdk "golang.org/x/oauth2"

	"github.com/nostosx/servex-contrib/auth/oauth2"
	"github.com/nostosx/servex-contrib/auth/oidc"
	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex-contrib/session"
)

// OIDC declares the complete provider flow. Resolve maps a verified provider
// identity to the application's own user record/principal and access policy.
type OIDC[P any] struct {
	Client   *oauth2.Client
	Verifier *oidc.Verifier
	Resolve  func(context.Context, *oidc.IDToken) (P, error)
}

// Config selects application principal validation and optional OIDC login.
// SuccessURL must be a local absolute path; the default is /. HTTPClient is
// copied at construction and receives a ten-second timeout when none is set.
type Config[P any] struct {
	SessionKey string
	Valid      func(P) bool
	OIDC       *OIDC[P]
	SuccessURL string
	HTTPClient *http.Client
}

// Manager restores application principals from sessions and coordinates login.
// Its session store and provider clients remain caller-owned.
type Manager[P any] struct {
	sessions *session.Manager
	config   Config[P]
}

// New requires a session manager and principal validator. OIDC is optional;
// when supplied, its client, verifier and application resolver must all be set.
func New[P any](sessions *session.Manager, config Config[P]) (*Manager[P], error) {
	if sessions == nil || config.Valid == nil {
		return nil, errors.New("login: session manager and principal validator required")
	}
	if config.SessionKey == "" {
		config.SessionKey = "auth.principal"
	}
	client := http.Client{Timeout: 10 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 10 * time.Second
		}
	}
	if client.Timeout < 0 {
		return nil, errors.New("login: HTTP client timeout must be positive")
	}
	config.HTTPClient = &client
	if config.SuccessURL == "" {
		config.SuccessURL = "/"
	}
	u, err := url.Parse(config.SuccessURL)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(config.SuccessURL, "/") || strings.HasPrefix(config.SuccessURL, "//") || strings.ContainsAny(config.SuccessURL, "\r\n\\") {
		return nil, errors.New("login: success URL must be a local absolute path")
	}
	if config.OIDC != nil {
		if config.OIDC.Client == nil || config.OIDC.Verifier == nil || config.OIDC.Resolve == nil {
			return nil, errors.New("login: complete OIDC provider configuration required")
		}
		copyProvider := *config.OIDC
		config.OIDC = &copyProvider
	}
	return &Manager[P]{sessions: sessions, config: config}, nil
}

// Middleware loads a session once and restores only a validated application
// principal. Mount it after CORS/CSRF and before routing. Native HTTP and endpoint
// handlers then read the same typed context as JWT authentication middleware.
func (m *Manager[P]) Middleware(next http.Handler) http.Handler {
	return m.sessions.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current, _ := session.FromContext(r.Context())
		var principal P
		found, err := current.Get(m.config.SessionKey, &principal)
		if err != nil {
			http.Error(w, "session unavailable", http.StatusServiceUnavailable)
			return
		}
		if found {
			if !m.config.Valid(principal) {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}
			r = r.WithContext(endpoint.NewPrincipalContext(r.Context(), principal))
		}
		next.ServeHTTP(w, r)
	}))
}

// SignIn also supports password/application authentication. Call only after
// credential verification, before writing the HTTP response. Token rotation and
// principal persistence commit atomically through the injected Session Store.
func (m *Manager[P]) SignIn(ctx context.Context, principal P) error {
	if !m.config.Valid(principal) {
		return errors.New("login: invalid application principal")
	}
	current, ok := session.FromContext(ctx)
	if !ok {
		return errors.New("login: session middleware missing")
	}
	if err := current.RenewToken(); err != nil {
		return err
	}
	if err := current.Remove(m.bindingKey()); err != nil {
		return err
	}
	return current.Put(m.config.SessionKey, principal)
}

// SignOut destroys the whole current session. Call it before writing the response
// so the session middleware can persist revocation and expire the browser cookie.
func (m *Manager[P]) SignOut(ctx context.Context) error {
	current, ok := session.FromContext(ctx)
	if !ok {
		return errors.New("login: session middleware missing")
	}
	return current.Destroy()
}

// Require rejects requests without a valid application principal. Mount it inside
// Middleware, which loads the session and installs the principal in the context.
func (m *Manager[P]) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := endpoint.PrincipalFromContext[P](r.Context())
		if !ok || !m.config.Valid(principal) {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Begin and Callback support query-based authorization-code callbacks. Browser
// state is bound to the already managed Session; no second binding cookie exists.
func (m *Manager[P]) Begin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if m.config.OIDC == nil {
		http.NotFound(w, r)
		return
	}
	current, ok := session.FromContext(r.Context())
	if !ok {
		http.Error(w, "login unavailable", http.StatusServiceUnavailable)
		return
	}
	binding := rand.Text()
	if err := current.Put(m.bindingKey(), binding); err != nil {
		http.Error(w, "login unavailable", http.StatusServiceUnavailable)
		return
	}
	authorization, err := m.config.OIDC.Client.Begin(r.Context(), binding)
	if err != nil {
		http.Error(w, "login unavailable", http.StatusServiceUnavailable)
		return
	}
	http.Redirect(w, r, authorization.URL, http.StatusFound)
}

// Callback handles a GET authorization-code response, verifies its session binding
// and OIDC nonce, and signs in the resolved application principal before redirecting.
// Mount it inside Middleware alongside Begin.
func (m *Manager[P]) Callback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if m.config.OIDC == nil {
		http.NotFound(w, r)
		return
	}
	current, ok := session.FromContext(r.Context())
	if !ok {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	var binding string
	found, err := current.Get(m.bindingKey(), &binding)
	if err != nil || !found {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	ctx := context.WithValue(r.Context(), sdk.HTTPClient, m.config.HTTPClient)
	result, err := m.config.OIDC.Client.Complete(ctx, binding, r.URL.Query().Get("state"), r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	raw, _ := result.Token.Extra("id_token").(string)
	token, err := m.config.OIDC.Verifier.VerifyNonce(r.Context(), raw, result.Nonce)
	if err != nil {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	principal, err := m.config.OIDC.Resolve(r.Context(), token)
	if err != nil || m.SignIn(r.Context(), principal) != nil {
		http.Error(w, "login failed", http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, m.config.SuccessURL, http.StatusSeeOther)
}

// Logout handles POST session revocation and returns 204 on success. Mount it
// inside Middleware and apply the application's CSRF policy to browser requests.
func (m *Manager[P]) Logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := m.SignOut(r.Context()); err != nil {
		http.Error(w, "logout unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager[P]) bindingKey() string { return m.config.SessionKey + ".oidc-binding" }
