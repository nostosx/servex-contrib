package client

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v3"
)

type providerTransport struct{ target *url.URL }

func (tr providerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copyRequest := request.Clone(request.Context())
	copyURL := *request.URL
	copyURL.Scheme, copyURL.Host = tr.target.Scheme, tr.target.Host
	copyRequest.URL = &copyURL
	return http.DefaultTransport.RoundTrip(copyRequest)
}

type providerFixture struct {
	mu        sync.Mutex
	issuer    string
	nonce     string
	challenge string
	changes   map[string]any
	profile   string
	emails    string
	redirect  bool
	calls     int
	key       *rsa.PrivateKey
	server    *httptest.Server
}

func newProviderFixture(t *testing.T, kind ProviderKind) (*providerFixture, *Provider) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f := &providerFixture{key: key, profile: `{"id":42,"login":"octocat","email":"untrusted@example.com"}`,
		emails: `[{"email":"bad@example.com","verified":false,"primary":true},{"email":"other@example.com","verified":true},{"email":"primary@example.com","verified":true,"primary":true}]`}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration"):
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": f.issuer,
				"authorization_endpoint": f.server.URL + "/authorize", "token_endpoint": f.server.URL + "/token", "jwks_uri": f.server.URL + "/keys"})
		case r.URL.Path == "/keys":
			_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &f.key.PublicKey, KeyID: "fixture", Algorithm: "RS256", Use: "sig"}}})
		case r.URL.Path == "/token" || strings.HasSuffix(r.URL.Path, "/access_token") || r.URL.Path == "/api/oauth2/token":
			f.calls++
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			digest := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if r.Form.Get("client_id") != "client" || r.Form.Get("client_secret") != "secret" || base64.RawURLEncoding.EncodeToString(digest[:]) != f.challenge {
				t.Error("missing client authentication or invalid PKCE")
			}
			claims := map[string]any{"iss": f.issuer, "sub": "stable-subject", "aud": "client", "nonce": f.nonce,
				"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "email": "user@example.com", "email_verified": true, "name": "User"}
			maps.Copy(claims, f.changes)
			signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: f.key, KeyID: "fixture"}}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			payload, _ := json.Marshal(claims)
			signed, err := signer.Sign(payload)
			if err != nil {
				t.Error(err)
				return
			}
			raw, _ := signed.CompactSerialize()
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "token_type": "Bearer", "expires_in": 3600, "id_token": raw})
		case r.URL.Path == "/user" || r.URL.Path == "/api/v10/users/@me":
			if r.Header.Get("Authorization") != "Bearer access" {
				t.Error("missing authenticated profile request")
			}
			if f.redirect {
				http.Redirect(w, r, "https://attacker.example/stolen", http.StatusFound)
				return
			}
			_, _ = io.WriteString(w, f.profile)
		case r.URL.Path == "/user/emails":
			if r.Header.Get("Authorization") != "Bearer access" {
				t.Error("missing authenticated email request")
			}
			_, _ = io.WriteString(w, f.emails)
		default:
			t.Errorf("unexpected backend path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	target, _ := url.Parse(f.server.URL)
	config := ProviderConfig{Kind: kind, ClientID: "client", ClientSecret: "secret", RedirectURL: "https://app.example/callback",
		Tenant: "12345678-1234-1234-1234-123456789abc", HTTPClient: &http.Client{Transport: providerTransport{target: target}}, AllowInsecureLoopback: true}
	if kind == OIDC {
		config.Issuer = f.server.URL
	}
	f.issuer, _, _, err = providerDefaults(config)
	if err != nil {
		t.Fatal(err)
	}
	store, _ := NewMemoryStore(20)
	provider, err := NewProvider(t.Context(), config, store)
	if err != nil {
		t.Fatal(err)
	}
	return f, provider
}

func (f *providerFixture) begin(t *testing.T, provider *Provider) string {
	t.Helper()
	authorization, err := provider.Begin(t.Context(), "browser")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(authorization.URL)
	f.mu.Lock()
	f.nonce, f.challenge = u.Query().Get("nonce"), u.Query().Get("code_challenge")
	f.mu.Unlock()
	if u.Query().Get("code_challenge_method") != "S256" || f.nonce == "" {
		t.Fatal("missing PKCE/nonce")
	}
	if provider.kind == Apple && u.Query().Get("response_mode") != "form_post" {
		t.Fatal("Apple requires form_post")
	}
	return u.Query().Get("state")
}

func TestProviderOIDCLogin(t *testing.T) {
	for _, kind := range []ProviderKind{Google, Microsoft, GitLab, LinkedIn, Apple, OIDC} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			f, provider := newProviderFixture(t, kind)
			state := f.begin(t, provider)
			if kind == Google || kind == Apple {
				f.mu.Lock()
				if kind == Google {
					f.changes = map[string]any{"iss": "accounts.google.com"}
				} else {
					f.changes = map[string]any{"email_verified": "true"}
				}
				f.mu.Unlock()
			}
			identity, err := provider.Complete(t.Context(), "browser", state, "code")
			if err != nil || identity.Subject != "stable-subject" || identity.Issuer != f.issuer || !identity.EmailVerified || identity.Provider != kind {
				t.Fatalf("identity=%+v error=%v", identity, err)
			}
			if _, err := provider.Complete(t.Context(), "browser", state, "code"); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("replay accepted: %v", err)
			}
		})
	}
}

func TestProviderRejectsInvalidIDTokens(t *testing.T) {
	for _, tt := range []struct {
		name  string
		claim string
		value any
	}{
		{name: "wrong issuer", claim: "iss", value: "https://attacker.example"},
		{name: "wrong audience", claim: "aud", value: "other-client"},
		{name: "wrong nonce", claim: "nonce", value: "other-login"},
		{name: "expired", claim: "exp", value: time.Now().Add(-time.Hour).Unix()},
		{name: "missing subject", claim: "sub", value: ""},
		{name: "wrong access token hash", claim: "at_hash", value: "wrong"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, provider := newProviderFixture(t, OIDC)
			state := f.begin(t, provider)
			f.mu.Lock()
			f.changes = map[string]any{tt.claim: tt.value}
			f.mu.Unlock()
			if _, err := provider.Complete(t.Context(), "browser", state, "code"); err == nil {
				t.Fatal("invalid ID token accepted")
			}
		})
	}
}

func TestProviderOAuthProfiles(t *testing.T) {
	for _, tt := range []struct {
		name      string
		kind      ProviderKind
		profile   string
		emails    string
		redirect  bool
		wantEmail string
		wantError bool
	}{
		{name: "GitHub verified primary", kind: GitHub, wantEmail: "primary@example.com"},
		{name: "GitHub no verified email", kind: GitHub, emails: `[{"email":"bad@example.com","primary":true,"verified":false}]`},
		{name: "GitHub missing ID", kind: GitHub, profile: `{"login":"name"}`, wantError: true},
		{name: "GitHub profile failure", kind: GitHub, profile: `{"id":42`, wantError: true},
		{name: "GitHub redirect", kind: GitHub, redirect: true, wantError: true},
		{name: "GitHub oversized", kind: GitHub, profile: strings.Repeat(" ", (1<<20)+1), wantError: true},
		{name: "Discord verified", kind: Discord, profile: `{"id":"12345","username":"user","email":"user@example.com","verified":true}`, wantEmail: "user@example.com"},
		{name: "Discord missing ID", kind: Discord, profile: `{"id":"invalid"}`, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, provider := newProviderFixture(t, tt.kind)
			f.mu.Lock()
			if tt.profile != "" {
				f.profile = tt.profile
			}
			if tt.emails != "" {
				f.emails = tt.emails
			}
			f.redirect = tt.redirect
			f.mu.Unlock()
			state := f.begin(t, provider)
			identity, err := provider.Complete(t.Context(), "browser", state, "code")
			if tt.wantError {
				if err == nil {
					t.Fatal("invalid profile accepted")
				}
				return
			}
			if err != nil || identity.Subject == "" || identity.Email != tt.wantEmail || identity.EmailVerified != (tt.wantEmail != "") {
				t.Fatalf("identity=%+v error=%v", identity, err)
			}
		})
	}
}

func TestProviderMixupAndInvalidConfiguration(t *testing.T) {
	f, first := newProviderFixture(t, GitHub)
	state := f.begin(t, first)
	second := *first
	second.kind = Discord
	if _, err := second.Complete(t.Context(), "browser", state, "code"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("provider mixup accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 0 {
		t.Fatal("provider mixup reached token endpoint")
	}
	for _, config := range []ProviderConfig{
		{Kind: "unknown"}, {Kind: Microsoft, Tenant: "common"}, {Kind: Microsoft, Tenant: "organizations"},
		{Kind: Google, Issuer: "https://attacker.example"}, {Kind: OIDC},
	} {
		if _, _, _, err := providerDefaults(config); err == nil {
			t.Fatalf("invalid config accepted: %+v", config)
		}
	}
}
