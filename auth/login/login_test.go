package login

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	sdk "golang.org/x/oauth2"

	oauthclient "github.com/nostosx/servex-contrib/auth/oauth2/client"
	"github.com/nostosx/servex-contrib/auth/oidc"
	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex-contrib/session"
)

type principal struct{ ID string }

func TestOIDCSessionComposition(t *testing.T) {
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var nonce atomic.Value
	nonce.Store("")
	var issuer string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/jwks" {
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public), "kid": "test", "alg": "EdDSA"}}})
			return
		}
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": issuer, "aud": "browser", "sub": "user-1", "nonce": nonce.Load().(string), "exp": time.Now().Add(time.Minute).Unix()})
		token.Header["kid"] = "test"
		raw, err := token.SignedString(key)
		if err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "provider-access", "token_type": "Bearer", "id_token": raw})
	}))
	defer provider.Close()
	issuer = provider.URL
	states, _ := oauthclient.NewMemoryStore(10)
	flow, err := oauthclient.New(oauthclient.Config{OAuth: sdk.Config{ClientID: "browser", RedirectURL: "https://app.example/callback", Endpoint: sdk.Endpoint{AuthURL: provider.URL + "/authorize", TokenURL: provider.URL + "/token", AuthStyle: sdk.AuthStyleInParams}}, AllowInsecureLoopback: true}, states)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := oidc.NewRemote(t.Context(), provider.URL, "browser", provider.URL+"/jwks", oidc.WithInsecureHTTP(), oidc.WithSigningAlgorithms("EdDSA"))
	if err != nil {
		t.Fatal(err)
	}
	store, _ := session.NewMemoryStore(10)
	sessions, _ := session.New(store)
	manager, err := New(sessions, Config[principal]{Valid: func(p principal) bool { return p.ID != "" }, OIDC: &OIDC[principal]{Client: flow, Verifier: verifier, Resolve: func(_ context.Context, token *oidc.IDToken) (principal, error) {
		return principal{ID: token.Subject}, nil
	}}, SuccessURL: "/private"})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", manager.Begin)
	mux.HandleFunc("GET /callback", manager.Callback)
	mux.HandleFunc("POST /logout", manager.Logout)
	mux.Handle("GET /private", manager.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := endpoint.PrincipalFromContext[principal](r.Context())
		_, _ = io.WriteString(w, p.ID)
	})))
	handler := manager.Middleware(mux)
	request := func(method, path string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://app.example"+path, nil)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	begin := request(http.MethodGet, "/login", nil)
	response := begin.Result()
	cookies := response.Cookies()
	_ = response.Body.Close()
	if begin.Code != http.StatusFound || len(cookies) != 1 {
		t.Fatal(begin.Code, cookies)
	}
	u, _ := url.Parse(begin.Header().Get("Location"))
	nonce.Store(u.Query().Get("nonce"))
	callbackPath := "/callback?code=accepted&state=" + url.QueryEscape(u.Query().Get("state"))
	callback := request(http.MethodGet, callbackPath, cookies[0])
	response = callback.Result()
	logged := response.Cookies()
	_ = response.Body.Close()
	if callback.Code != http.StatusSeeOther || len(logged) != 1 || logged[0].Value == cookies[0].Value {
		t.Fatal("login/rotation failed", callback.Code, logged)
	}
	if got := request(http.MethodGet, "/private", logged[0]); got.Code != http.StatusOK || got.Body.String() != "user-1" {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := request(http.MethodGet, callbackPath, cookies[0]); got.Code != http.StatusUnauthorized {
		t.Fatal("callback replay accepted", got.Code)
	}
	if got := request(http.MethodPost, "/logout", logged[0]); got.Code != http.StatusNoContent {
		t.Fatal(got.Code)
	}
	if got := request(http.MethodGet, "/private", logged[0]); got.Code != http.StatusUnauthorized {
		t.Fatal("logout failed", got.Code)
	}
}

type providerTestTransport func(*http.Request) (*http.Response, error)

func (f providerTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderSessionComposition(t *testing.T) {
	for _, kind := range []oauthclient.ProviderKind{oauthclient.GitHub, oauthclient.Apple} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			var nonce atomic.Value
			nonce.Store("")
			transport := providerTestTransport(func(r *http.Request) (*http.Response, error) {
				var payload any
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					payload = map[string]string{"issuer": "https://appleid.apple.com", "authorization_endpoint": "https://appleid.apple.com/auth/authorize", "token_endpoint": "https://appleid.apple.com/auth/token", "jwks_uri": "https://appleid.apple.com/auth/keys"}
				case "/auth/keys":
					payload = map[string]any{"keys": []any{map[string]string{"kty": "RSA", "kid": "test", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}}
				case "/auth/token", "/login/oauth/access_token":
					token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": "https://appleid.apple.com", "aud": "client", "sub": "external-id", "nonce": nonce.Load().(string), "exp": time.Now().Add(time.Minute).Unix()})
					token.Header["kid"] = "test"
					raw, err := token.SignedString(key)
					if err != nil {
						return nil, err
					}
					payload = map[string]string{"access_token": "access", "token_type": "Bearer", "id_token": raw}
				case "/user":
					payload = map[string]any{"id": 42, "login": "external-user"}
				case "/user/emails":
					payload = []any{}
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
				data, err := json.Marshal(payload)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, err
			})
			states, _ := oauthclient.NewMemoryStore(10)
			flow, err := oauthclient.NewProvider(t.Context(), oauthclient.ProviderConfig{Kind: kind, ClientID: "client", ClientSecret: "secret", RedirectURL: "https://app.example/callback", HTTPClient: &http.Client{Transport: transport}}, states)
			if err != nil {
				t.Fatal(err)
			}
			store, _ := session.NewMemoryStore(10)
			sessions, _ := session.New(store, session.WithSameSite(http.SameSiteNoneMode))
			manager, err := New(sessions, Config[principal]{Valid: func(p principal) bool { return p.ID != "" }, Provider: &Provider[principal]{Client: flow, Resolve: func(_ context.Context, identity oauthclient.Identity) (principal, error) {
				return principal{ID: identity.Subject}, nil
			}}})
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/login", manager.Begin)
			mux.HandleFunc("/callback", manager.Callback)
			mux.Handle("/private", manager.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p, _ := endpoint.PrincipalFromContext[principal](r.Context())
				_, _ = io.WriteString(w, p.ID)
			})))
			handler := manager.Middleware(mux)
			begin := httptest.NewRecorder()
			handler.ServeHTTP(begin, httptest.NewRequest(http.MethodGet, "https://app.example/login", nil))
			response := begin.Result()
			cookies := response.Cookies()
			_ = response.Body.Close()
			if begin.Code != http.StatusFound || len(cookies) != 1 {
				t.Fatal(begin.Code, cookies)
			}
			redirect, _ := url.Parse(begin.Header().Get("Location"))
			nonce.Store(redirect.Query().Get("nonce"))
			values := url.Values{"code": []string{"code"}, "state": []string{redirect.Query().Get("state")}}
			callbackURL := "https://app.example/callback"
			var body io.Reader
			if flow.CallbackMethod() == http.MethodPost {
				body = strings.NewReader(values.Encode())
			} else {
				callbackURL += "?" + values.Encode()
			}
			request := httptest.NewRequest(flow.CallbackMethod(), callbackURL, body)
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.AddCookie(cookies[0])
			callback := httptest.NewRecorder()
			handler.ServeHTTP(callback, request)
			response = callback.Result()
			logged := response.Cookies()
			_ = response.Body.Close()
			if callback.Code != http.StatusSeeOther || len(logged) != 1 || logged[0].Value == cookies[0].Value {
				t.Fatal("login/rotation failed", callback.Code, logged)
			}
			request = httptest.NewRequest(http.MethodGet, "https://app.example/private", nil)
			request.AddCookie(logged[0])
			private := httptest.NewRecorder()
			handler.ServeHTTP(private, request)
			expected := "42"
			if kind == oauthclient.Apple {
				expected = "external-id"
			}
			if private.Code != http.StatusOK || private.Body.String() != expected {
				t.Fatal(private.Code, private.Body.String())
			}
			if kind == oauthclient.Apple {
				for _, tt := range []struct {
					name, contentType, body string
					status                  int
				}{
					{"wrong content type", "application/json", "{}", http.StatusBadRequest},
					{"oversized form", "application/x-www-form-urlencoded", "state=" + strings.Repeat("x", 17<<10), http.StatusBadRequest},
					{"duplicate state", "application/x-www-form-urlencoded", "state=a&state=b&code=c", http.StatusUnauthorized},
				} {
					t.Run(tt.name, func(t *testing.T) {
						r := httptest.NewRequest(http.MethodPost, "https://app.example/callback", strings.NewReader(tt.body))
						r.Header.Set("Content-Type", tt.contentType)
						r.AddCookie(cookies[0])
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						if w.Code != tt.status {
							t.Fatal(w.Code)
						}
					})
				}
			}
		})
	}
}
