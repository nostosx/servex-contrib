package oidc_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/auth/oidc"
)

type identityProvider struct {
	server   *httptest.Server
	mu       sync.Mutex
	key      *rsa.PrivateKey
	kid      string
	requests atomic.Int32
}

func newIdentityProvider(t *testing.T) *identityProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &identityProvider{key: key, kid: "first"}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			if err := json.NewEncoder(w).Encode(map[string]any{"issuer": p.server.URL, "jwks_uri": p.server.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}}); err != nil {
				t.Error(err)
			}
		case "/keys":
			p.requests.Add(1)
			p.mu.Lock()
			key := map[string]any{"kty": "RSA", "kid": p.kid, "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes())}
			p.mu.Unlock()
			if err := json.NewEncoder(w).Encode(map[string]any{"keys": []any{key}}); err != nil {
				t.Error(err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *identityProvider) token(t *testing.T, changes map[string]any) string {
	t.Helper()
	claims := map[string]any{"iss": p.server.URL, "sub": "user-1", "aud": "client", "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "nonce": "nonce-1", "name": "Example"}
	for key, value := range changes {
		if value == nil {
			delete(claims, key)
		} else {
			claims[key] = value
		}
	}
	p.mu.Lock()
	key, kid := p.key, p.kid
	p.mu.Unlock()
	return signToken(t, key, kid, claims)
}

func signToken(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func TestVerifyDiscoveryAndRemote(t *testing.T) {
	for _, discovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit jwks", true: "discovery"}[discovery], func(t *testing.T) {
			p := newIdentityProvider(t)
			var verifier *oidc.Verifier
			var err error
			if discovery {
				verifier, err = oidc.New(t.Context(), p.server.URL, "client", oidc.WithInsecureHTTP())
			} else {
				verifier, err = oidc.NewRemote(t.Context(), p.server.URL, "client", p.server.URL+"/keys", oidc.WithInsecureHTTP())
			}
			if err != nil {
				t.Fatal(err)
			}
			raw := p.token(t, nil)
			for range 2 {
				verified, err := verifier.Verify(t.Context(), raw)
				if err != nil || verified.Subject != "user-1" || verified.Issuer != p.server.URL {
					t.Fatalf("verified claims = %v, %v", verified, err)
				}
				var extra struct {
					Name string `json:"name"`
				}
				if err := verified.Claims(&extra); err != nil || extra.Name != "Example" {
					t.Fatalf("custom claims = %+v, %v", extra, err)
				}
			}
			if p.requests.Load() != 1 {
				t.Errorf("jwks requests = %d, want one cached key set", p.requests.Load())
			}
			if _, err := verifier.VerifyNonce(t.Context(), raw, "nonce-1"); err != nil {
				t.Fatal(err)
			}
			for _, nonce := range []string{"", "other"} {
				if _, err := verifier.VerifyNonce(t.Context(), raw, nonce); err == nil {
					t.Errorf("nonce %q accepted", nonce)
				}
			}
		})
	}
}

func TestRejectInvalidClaimsAndSignatures(t *testing.T) {
	p := newIdentityProvider(t)
	verifier, err := oidc.New(t.Context(), p.server.URL, "client", oidc.WithInsecureHTTP())
	if err != nil {
		t.Fatal(err)
	}
	for name, changes := range map[string]map[string]any{
		"issuer":                         {"iss": "https://other.example"},
		"audience":                       {"aud": "other"},
		"expiry":                         {"exp": time.Now().Add(-time.Hour).Unix()},
		"missing expiry":                 {"exp": nil},
		"subject":                        {"sub": nil},
		"future nbf":                     {"nbf": time.Now().Add(time.Hour).Unix()},
		"authorized party":               {"azp": "other"},
		"multiple audiences without azp": {"aud": []string{"client", "other"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.Verify(t.Context(), p.token(t, changes)); err == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
	if _, err := verifier.Verify(t.Context(), p.token(t, map[string]any{"aud": []string{"client", "other"}, "azp": "client"})); err != nil {
		t.Fatalf("valid authorized party rejected: %v", err)
	}
	valid := p.token(t, nil)
	parts := strings.Split(valid, ".")
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"` + p.server.URL + `","sub":"admin","aud":"client","exp":9999999999}`))
	if _, err := verifier.Verify(t.Context(), strings.Join(parts, ".")); err == nil {
		t.Fatal("tampered payload accepted")
	}
	parts = strings.Split(valid, ".")
	parts[0] = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	parts[2] = ""
	if _, err := verifier.Verify(t.Context(), strings.Join(parts, ".")); err == nil {
		t.Fatal("unsigned token accepted")
	}
}

func TestJWKSRotationAndCancellation(t *testing.T) {
	p := newIdentityProvider(t)
	verifier, err := oidc.NewRemote(t.Context(), p.server.URL, "client", p.server.URL+"/keys", oidc.WithInsecureHTTP())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := verifier.Verify(ctx, p.token(t, nil)); !errors.Is(err, context.Canceled) || p.requests.Load() != 0 {
		t.Fatalf("cancelled verification = %v, requests = %d", err, p.requests.Load())
	}
	if _, err := verifier.Verify(t.Context(), p.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.key, p.kid = key, "rotated"
	p.mu.Unlock()
	if _, err := verifier.Verify(t.Context(), p.token(t, nil)); err != nil {
		t.Fatalf("rotated key rejected: %v", err)
	}
	if p.requests.Load() != 2 {
		t.Errorf("jwks requests = %d, want refresh for unknown kid", p.requests.Load())
	}
}

func TestVerifyConcurrentKeyFetch(t *testing.T) {
	p := newIdentityProvider(t)
	verifier, err := oidc.NewRemote(t.Context(), p.server.URL, "client", p.server.URL+"/keys", oidc.WithInsecureHTTP())
	if err != nil {
		t.Fatal(err)
	}
	raw := p.token(t, nil)
	errors := make(chan error, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			_, err := verifier.Verify(t.Context(), raw)
			errors <- err
		})
	}
	group.Wait()
	for range 16 {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}
	if p.requests.Load() != 1 {
		t.Errorf("parallel key fetches = %d, want one shared fetch", p.requests.Load())
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestCancellationWhileJWKSFetchIsShared(t *testing.T) {
	p := newIdentityProvider(t)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return http.DefaultTransport.RoundTrip(request)
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	})}
	verifier, err := oidc.NewRemote(t.Context(), p.server.URL, "client", p.server.URL+"/keys", oidc.WithInsecureHTTP(), oidc.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	raw := p.token(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := verifier.Verify(ctx, raw)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("jwks fetch did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("verification ignored caller cancellation")
	}
	close(release)
	if _, err := verifier.Verify(t.Context(), raw); err != nil {
		t.Fatalf("shared jwks fetch did not remain usable: %v", err)
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name, issuer, audience, jwks string
		options                      []oidc.Option
	}{
		{name: "http issuer", issuer: "http://issuer.example", audience: "client", jwks: "https://issuer.example/keys"},
		{name: "relative issuer", issuer: "/issuer", audience: "client", jwks: "https://issuer.example/keys"},
		{name: "issuer userinfo", issuer: "https://user@issuer.example", audience: "client", jwks: "https://issuer.example/keys"},
		{name: "issuer query", issuer: "https://issuer.example?tenant=x", audience: "client", jwks: "https://issuer.example/keys"},
		{name: "audience", issuer: "https://issuer.example", jwks: "https://issuer.example/keys"},
		{name: "http jwks", issuer: "https://issuer.example", audience: "client", jwks: "http://issuer.example/keys"},
		{name: "jwks fragment", issuer: "https://issuer.example", audience: "client", jwks: "https://issuer.example/keys#fragment"},
		{name: "hmac", issuer: "https://issuer.example", audience: "client", jwks: "https://issuer.example/keys", options: []oidc.Option{oidc.WithSigningAlgorithms("HS256")}},
		{name: "none", issuer: "https://issuer.example", audience: "client", jwks: "https://issuer.example/keys", options: []oidc.Option{oidc.WithSigningAlgorithms("none")}},
		{name: "empty algorithms", issuer: "https://issuer.example", audience: "client", jwks: "https://issuer.example/keys", options: []oidc.Option{oidc.WithSigningAlgorithms()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := oidc.NewRemote(t.Context(), tt.issuer, tt.audience, tt.jwks, tt.options...); err == nil {
				t.Fatal("invalid verifier configuration accepted")
			}
		})
	}
}

func TestDiscoveryRejectsHTTPDowngrade(t *testing.T) {
	var insecureCalls atomic.Int32
	insecure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { insecureCalls.Add(1) }))
	defer insecure.Close()
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, insecure.URL, http.StatusFound)
	}))
	defer secure.Close()
	client := secure.Client()
	if _, err := oidc.New(t.Context(), secure.URL, "client", oidc.WithHTTPClient(client)); err == nil {
		t.Fatal("https-to-http downgrade accepted")
	}
	if insecureCalls.Load() != 0 {
		t.Fatal("followed a cleartext redirect")
	}
	if client.CheckRedirect != nil || client.Timeout != 0 {
		t.Fatal("mutated the caller's http client")
	}
}
