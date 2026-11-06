package client

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func TestCodeFlowPKCEBindingAndReplay(t *testing.T) {
	var challenge string
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		digest := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(digest[:]) != challenge {
			t.Error("PKCE verifier mismatch")
		}
		if r.Form.Get("code") != "code" || r.Form.Get("redirect_uri") != "http://localhost/callback" {
			t.Error(r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access","token_type":"Bearer","expires_in":3600,"id_token":"must-be-verified"}`))
	}))
	defer server.Close()
	store, _ := NewMemoryStore(10)
	config := Config{OAuth: sdk.Config{ClientID: "client", RedirectURL: "http://localhost/callback", Endpoint: sdk.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token", AuthStyle: sdk.AuthStyleInParams}}, AllowInsecureLoopback: true}
	client, err := New(config, store)
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := client.Begin(t.Context(), "trusted-browser-secret")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(authorization.URL)
	state := u.Query().Get("state")
	challenge = u.Query().Get("code_challenge")
	if state == "" || challenge == "" || u.Query().Get("code_challenge_method") != "S256" {
		t.Fatal(u)
	}
	result, err := client.Complete(t.Context(), "trusted-browser-secret", state, "code")
	if err != nil || result.Token.AccessToken != "access" || result.Nonce != u.Query().Get("nonce") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := client.Complete(t.Context(), "trusted-browser-secret", state, "code"); !errors.Is(err, ErrInvalidState) {
		t.Fatal("replay accepted", err)
	}
	authorization, _ = client.Begin(t.Context(), "browser-one")
	u, _ = url.Parse(authorization.URL)
	if _, err := client.Complete(t.Context(), "browser-two", u.Query().Get("state"), "code"); !errors.Is(err, ErrInvalidState) {
		t.Fatal("wrong browser accepted", err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid state reached token endpoint")
	}
	config.AllowInsecureLoopback = false
	if _, err := New(config, store); err == nil {
		t.Fatal("HTTP accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.Begin(ctx, "browser"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStateStoreExpiryCapacityAndSingleConsumption(t *testing.T) {
	store, _ := NewMemoryStore(1)
	live := Attempt{ExpiresAt: time.Now().Add(time.Minute)}
	if err := store.Save(t.Context(), "one", live); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "two", live); err == nil {
		t.Fatal("capacity ignored")
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := store.Take(t.Context(), "one"); err == nil {
				winners.Add(1)
			}
		})
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal(winners.Load())
	}
	store.states["expired"] = Attempt{ExpiresAt: time.Now().Add(-time.Second)}
	if _, err := store.Take(t.Context(), "expired"); !errors.Is(err, ErrInvalidState) {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), "new", live); err != nil {
		t.Fatal(err)
	}
}

func TestTokenRefresh(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "refresh" {
			t.Error(r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"renewed","token_type":"Bearer","expires_in":3600}`))
	}))
	defer server.Close()
	store, _ := NewMemoryStore(1)
	client, err := New(Config{OAuth: sdk.Config{ClientID: "client", RedirectURL: "http://localhost/callback",
		Endpoint: sdk.Endpoint{AuthURL: server.URL + "/auth", TokenURL: server.URL + "/token", AuthStyle: sdk.AuthStyleInParams}}, AllowInsecureLoopback: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	source := client.TokenSource(t.Context(), &sdk.Token{AccessToken: "expired", RefreshToken: "refresh", Expiry: time.Now().Add(-time.Hour)})
	for range 2 {
		token, err := source.Token()
		if err != nil || token.AccessToken != "renewed" {
			t.Fatal(token, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("refreshed token not cached")
	}
}

func TestClientCredentialsRequiresCredentialsAndHTTPS(t *testing.T) {
	for _, tt := range []struct {
		name   string
		id     string
		secret string
		url    string
	}{
		{name: "missing client ID", secret: "secret", url: "https://provider.example/token"},
		{name: "missing secret", id: "client", url: "https://provider.example/token"},
		{name: "missing endpoint", id: "client", secret: "secret"},
		{name: "HTTP loopback", id: "client", secret: "secret", url: "http://localhost/token"},
		{name: "URL credentials", id: "client", secret: "secret", url: "https://user:password@provider.example/token"},
		{name: "fragment", id: "client", secret: "secret", url: "https://provider.example/token#secret"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ClientCredentials(t.Context(), clientcredentials.Config{
				ClientID: tt.id, ClientSecret: tt.secret, TokenURL: tt.url,
			})
			if err == nil || source != nil {
				t.Fatalf("invalid credentials/endpoint accepted: source=%v err=%v", source, err)
			}
		})
	}
}

func TestClientCredentialsSnapshotsConfigAndPropagatesCancellation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "read" || r.Form.Get("audience") != "catalog" {
			t.Error("client-credentials request did not retain its configured parameters", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"service-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), sdk.HTTPClient, server.Client()), 5*time.Second)
	defer cancel()
	config := clientcredentials.Config{
		ClientID: "client", ClientSecret: "secret", TokenURL: server.URL,
		Scopes: []string{"read"}, EndpointParams: url.Values{"audience": {"catalog"}}, AuthStyle: sdk.AuthStyleInParams,
	}
	source, err := ClientCredentials(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	config.Scopes[0] = "write"
	config.EndpointParams["audience"][0] = "other"
	token, err := source.Token()
	if err != nil || token.AccessToken != "service-token" {
		t.Fatalf("token=%v err=%v", token, err)
	}
	cancel()
	canceled, err := ClientCredentials(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canceled.Token(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled source returned %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("canceled source reached the token endpoint", calls.Load())
	}
}
