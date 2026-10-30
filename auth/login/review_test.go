package login

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	jwt "github.com/golang-jwt/jwt/v5"
	goredis "github.com/redis/go-redis/v9"
	sdk "golang.org/x/oauth2"

	"github.com/nostosx/servex-contrib/auth/oauth2"
	"github.com/nostosx/servex-contrib/auth/oauth2redis"
	"github.com/nostosx/servex-contrib/auth/oidc"
	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex-contrib/session"
	sessionredis "github.com/nostosx/servex-contrib/session/redis"
)

type reviewPrincipal struct{ ID string }

type reviewLogin struct {
	handlers     [2]http.Handler
	nonce        atomic.Value
	challenge    atomic.Value
	exchanges    atomic.Int32
	failRotation atomic.Bool
}

type reviewSessionStore struct {
	session.Store
	failRotation *atomic.Bool
}

func (s *reviewSessionStore) Rotate(ctx context.Context, oldKey, newKey string, expected uint64, record session.Record) error {
	if s.failRotation.Load() {
		return errors.New("review: session persistence unavailable")
	}
	return s.Store.Rotate(ctx, oldKey, newKey, expected, record)
}

func newReviewLogin(t *testing.T) *reviewLogin {
	t.Helper()
	fixture := &reviewLogin{}
	fixture.nonce.Store("")
	fixture.challenge.Store("")
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var issuer atomic.Value
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/jwks" {
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
				"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public),
				"kid": "review", "alg": "EdDSA",
			}}})
			return
		}
		fixture.exchanges.Add(1)
		if r.URL.Path != "/token" || r.Method != http.MethodPost {
			t.Error("unexpected provider request", r.Method, r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		challenge := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(challenge[:]) != fixture.challenge.Load().(string) {
			t.Error("PKCE challenge mismatch")
		}
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
			"iss": issuer.Load().(string), "aud": "browser", "sub": "shared-user",
			"nonce": fixture.nonce.Load().(string), "exp": time.Now().Add(time.Minute).Unix(),
		})
		token.Header["kid"] = "review"
		raw, err := token.SignedString(key)
		if err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "review-access", "token_type": "Bearer", "id_token": raw,
		})
	}))
	t.Cleanup(provider.Close)
	issuer.Store(provider.URL)
	server := miniredis.RunT(t)
	for index := range fixture.handlers {
		client := goredis.NewClient(&goredis.Options{
			Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true,
			ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		})
		t.Cleanup(func() { _ = client.Close() })
		states, err := oauth2redis.New(client, "review:oauth:")
		if err != nil {
			t.Fatal(err)
		}
		flow, err := oauth2.New(oauth2.Config{
			OAuth: sdk.Config{ClientID: "browser", RedirectURL: "https://app.example/callback",
				Endpoint: sdk.Endpoint{AuthURL: provider.URL + "/authorize", TokenURL: provider.URL + "/token", AuthStyle: sdk.AuthStyleInParams}},
			AllowInsecureLoopback: true,
		}, states)
		if err != nil {
			t.Fatal(err)
		}
		verifier, err := oidc.NewRemote(t.Context(), provider.URL, "browser", provider.URL+"/jwks",
			oidc.WithInsecureHTTP(), oidc.WithSigningAlgorithms("EdDSA"))
		if err != nil {
			t.Fatal(err)
		}
		store, err := sessionredis.New(client, sessionredis.WithPrefix("review:session:"))
		if err != nil {
			t.Fatal(err)
		}
		wrapped := &reviewSessionStore{Store: store, failRotation: &fixture.failRotation}
		sessions, err := session.New(wrapped)
		if err != nil {
			t.Fatal(err)
		}
		manager, err := New(sessions, Config[reviewPrincipal]{
			Valid: func(principal reviewPrincipal) bool { return principal.ID != "" },
			OIDC: &OIDC[reviewPrincipal]{Client: flow, Verifier: verifier,
				Resolve: func(_ context.Context, token *oidc.IDToken) (reviewPrincipal, error) {
					return reviewPrincipal{ID: token.Subject}, nil
				}},
			SuccessURL: "/private",
		})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /login", manager.Begin)
		mux.HandleFunc("GET /callback", manager.Callback)
		mux.HandleFunc("POST /logout", manager.Logout)
		mux.Handle("GET /private", manager.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, _ := auth.FromContext[reviewPrincipal](r.Context())
			_, _ = io.WriteString(w, principal.ID)
		})))
		fixture.handlers[index] = manager.Middleware(mux)
	}
	return fixture
}

func (f *reviewLogin) request(index int, method, path string, cookie *http.Cookie) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "https://app.example"+path, nil)
	if cookie != nil {
		request.AddCookie(cookie)
	}
	response := httptest.NewRecorder()
	f.handlers[index].ServeHTTP(response, request)
	return response
}

func reviewCookies(t *testing.T, response *httptest.ResponseRecorder) []*http.Cookie {
	t.Helper()
	result := response.Result()
	defer result.Body.Close()
	return result.Cookies()
}

func (f *reviewLogin) begin(t *testing.T) (*http.Cookie, string) {
	t.Helper()
	response := f.request(0, http.MethodGet, "/login", nil)
	cookies := reviewCookies(t, response)
	if response.Code != http.StatusFound || len(cookies) != 1 {
		t.Fatal("login did not persist a binding session", response.Code, len(cookies))
	}
	authorization, err := url.Parse(response.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	f.nonce.Store(authorization.Query().Get("nonce"))
	f.challenge.Store(authorization.Query().Get("code_challenge"))
	callback := "/callback?code=accepted&state=" + url.QueryEscape(authorization.Query().Get("state"))
	return cookies[0], callback
}

func TestRedisSharedLoginAcrossManagers(t *testing.T) {
	fixture := newReviewLogin(t)
	anonymous, callbackPath := fixture.begin(t)
	callback := fixture.request(1, http.MethodGet, callbackPath, anonymous)
	cookies := reviewCookies(t, callback)
	if callback.Code != http.StatusSeeOther || len(cookies) != 1 {
		t.Fatal("callback on the other manager failed", callback.Code, len(cookies))
	}
	authenticated := cookies[0]
	if authenticated.Value == anonymous.Value {
		t.Fatal("login did not rotate the session token")
	}
	private := fixture.request(0, http.MethodGet, "/private", authenticated)
	if private.Code != http.StatusOK || private.Body.String() != "shared-user" {
		t.Fatal("shared application principal missing", private.Code, private.Body.String())
	}
	if replay := fixture.request(0, http.MethodGet, callbackPath, anonymous); replay.Code != http.StatusUnauthorized {
		t.Fatal("old callback replay accepted", replay.Code)
	}
	if fixture.exchanges.Load() != 1 {
		t.Fatal("callback replay reached the token endpoint")
	}
	if logout := fixture.request(1, http.MethodPost, "/logout", authenticated); logout.Code != http.StatusNoContent {
		t.Fatal("logout failed", logout.Code)
	}
	if private := fixture.request(0, http.MethodGet, "/private", authenticated); private.Code != http.StatusUnauthorized {
		t.Fatal("logout did not revoke the other manager's session", private.Code)
	}
}

func TestConsumedStateCannotReplayAfterSessionCommitFailure(t *testing.T) {
	fixture := newReviewLogin(t)
	anonymous, callbackPath := fixture.begin(t)
	fixture.failRotation.Store(true)
	callback := fixture.request(1, http.MethodGet, callbackPath, anonymous)
	if callback.Code != http.StatusInternalServerError || len(reviewCookies(t, callback)) != 0 {
		t.Fatal("failed Session persistence returned login success", callback.Code)
	}
	if fixture.exchanges.Load() != 1 {
		t.Fatal("OIDC exchange was not reached before the forced commit failure")
	}
	private := fixture.request(0, http.MethodGet, "/private", anonymous)
	if private.Code != http.StatusUnauthorized {
		t.Fatal("failed commit persisted an authenticated principal", private.Code)
	}
	fixture.failRotation.Store(false)
	replay := fixture.request(0, http.MethodGet, callbackPath, anonymous)
	if replay.Code != http.StatusUnauthorized || fixture.exchanges.Load() != 1 {
		t.Fatal("consumed state was reusable after commit failure", replay.Code, fixture.exchanges.Load())
	}
}
