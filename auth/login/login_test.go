package login

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	sdk "golang.org/x/oauth2"

	"github.com/nostosx/servex-contrib/auth/oauth2"
	"github.com/nostosx/servex-contrib/auth/oidc"
	"github.com/nostosx/servex-contrib/middleware/auth"
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
	states, _ := oauth2.NewMemoryStore(10)
	flow, err := oauth2.New(oauth2.Config{OAuth: sdk.Config{ClientID: "browser", RedirectURL: "https://app.example/callback", Endpoint: sdk.Endpoint{AuthURL: provider.URL + "/authorize", TokenURL: provider.URL + "/token", AuthStyle: sdk.AuthStyleInParams}}, AllowInsecureLoopback: true}, states)
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
		p, _ := auth.FromContext[principal](r.Context())
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
