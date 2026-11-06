package oauth2server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ory/fosite"
	"github.com/ory/fosite/handler/openid"
	"github.com/ory/fosite/storage"
)

func testProvider(t *testing.T) (*Server, *storage.MemoryStore) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewMemoryStore()
	store.Clients["public"] = &fosite.DefaultClient{ID: "public", Public: true, RedirectURIs: []string{"https://client.example/callback"}, GrantTypes: []string{"authorization_code", "refresh_token"}, ResponseTypes: []string{"code"}, Scopes: []string{"openid", "offline_access", "profile"}}
	server, err := New(Config{Issuer: "https://issuer.example", Key: key, KeyID: "key-1", OAuth: fosite.Config{GlobalSecret: []byte(strings.Repeat("s", 32)), AccessTokenLifespan: time.Minute, RefreshTokenLifespan: time.Hour, AuthorizeCodeLifespan: time.Minute}}, store)
	if err != nil {
		t.Fatal(err)
	}
	return server, store
}

func authorize(t *testing.T, s *Server, verifier string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(verifier))
	query := url.Values{"client_id": {"public"}, "redirect_uri": {"https://client.example/callback"}, "response_type": {"code"}, "scope": {"openid offline_access profile"}, "state": {"strong-state-value"}, "nonce": {"strong-nonce-value"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(sum[:])}, "code_challenge_method": {"S256"}}
	r := httptest.NewRequest(http.MethodGet, "https://issuer.example/oauth2/authorize?"+query.Encode(), nil)
	request, err := s.Provider.NewAuthorizeRequest(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range request.GetRequestedScopes() {
		request.GrantScope(scope)
	}
	session := openid.NewDefaultSession()
	session.Subject = "user-1"
	session.Claims.Subject = "user-1"
	session.Claims.AuthTime = time.Now().Add(-time.Minute)
	response, err := s.Provider.NewAuthorizeResponse(t.Context(), request, session)
	if err != nil {
		t.Fatal(err)
	}
	return response.GetCode()
}

func tokenRequest(t *testing.T, s *Server, values url.Values) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "https://issuer.example/oauth2/token", strings.NewReader(values.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.Token(w, r)
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return w.Code, result
}

func TestPKCECodeRefreshAndReuse(t *testing.T) {
	s, _ := testProvider(t)
	verifier := strings.Repeat("v", 64)
	code := authorize(t, s, verifier)
	status, tokens := tokenRequest(t, s, url.Values{"grant_type": {"authorization_code"}, "client_id": {"public"}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {"https://client.example/callback"}})
	if status != 200 {
		t.Fatalf("code exchange: %d %v", status, tokens)
	}
	if tokens["id_token"] == nil || tokens["access_token"] == nil || tokens["refresh_token"] == nil {
		t.Fatal("missing protocol tokens")
	}
	oldRefresh := tokens["refresh_token"].(string)
	status, renewed := tokenRequest(t, s, url.Values{"grant_type": {"refresh_token"}, "client_id": {"public"}, "refresh_token": {oldRefresh}})
	if status != 200 || renewed["refresh_token"] == oldRefresh {
		t.Fatal("refresh did not rotate", status, renewed)
	}
	status, result := tokenRequest(t, s, url.Values{"grant_type": {"refresh_token"}, "client_id": {"public"}, "refresh_token": {oldRefresh}})
	if status != 400 || result["error"] != "invalid_grant" {
		t.Fatal("reused refresh accepted", status, result)
	}
	if _, _, err := s.Provider.IntrospectToken(t.Context(), renewed["access_token"].(string), fosite.AccessToken, openid.NewDefaultSession()); err == nil {
		t.Fatal("reused refresh did not revoke grant access tokens")
	}
}

func TestWrongVerifierAndRedirectRejected(t *testing.T) {
	for _, name := range []string{"verifier", "redirect"} {
		t.Run(name, func(t *testing.T) {
			s, _ := testProvider(t)
			verifier := strings.Repeat("v", 64)
			code := authorize(t, s, verifier)
			values := url.Values{"grant_type": {"authorization_code"}, "client_id": {"public"}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {"https://client.example/callback"}}
			if name == "verifier" {
				values.Set("code_verifier", strings.Repeat("x", 64))
			} else {
				values.Set("redirect_uri", "https://attacker.example/callback")
			}
			if status, _ := tokenRequest(t, s, values); status == 200 {
				t.Fatal("invalid credential accepted")
			}
		})
	}
}

func TestDiscoveryDoesNotExposePrivateKeys(t *testing.T) {
	s, _ := testProvider(t)
	w := httptest.NewRecorder()
	s.JWKS(w, httptest.NewRequest(http.MethodGet, "https://issuer.example/oauth2/jwks", nil))
	var keys struct{ Keys []map[string]any }
	if err := json.Unmarshal(w.Body.Bytes(), &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys.Keys) != 1 || keys.Keys[0]["kid"] != "key-1" || keys.Keys[0]["d"] != nil || keys.Keys[0]["p"] != nil {
		t.Fatal("invalid public key set")
	}
	w = httptest.NewRecorder()
	s.Discovery(w, httptest.NewRequest(http.MethodGet, "https://issuer.example/.well-known/openid-configuration", nil))
	var discovery map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &discovery); err != nil {
		t.Fatal(err)
	}
	if discovery["issuer"] != "https://issuer.example" || discovery["jwks_uri"] != "https://issuer.example/oauth2/jwks" {
		t.Fatal(discovery)
	}
}

func TestRejectsInsecureConfiguration(t *testing.T) {
	_, err := New(Config{Issuer: "http://external.example"}, storage.NewMemoryStore())
	if err == nil {
		t.Fatal("insecure issuer accepted")
	}
}
