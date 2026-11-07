package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ory/fosite"
	"github.com/ory/fosite/handler/openid"
)

func TestFileConfiguredProviderAcrossInstances(t *testing.T) {
	_, store := testProvider(t)
	dir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultServerConfig()
	cfg.Issuer, cfg.KeyID = "https://issuer.example", "persistent"
	cfg.SigningKeyFile, cfg.HMACSecretFile = filepath.Join(dir, "key.pem"), filepath.Join(dir, "hmac.key")
	if err := os.WriteFile(cfg.SigningKeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.HMACSecretFile, []byte(strings.Repeat("h", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := NewServerFromConfig(cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	before, after := false, false
	second, err := NewServerFromConfig(cfg, store,
		WithBeforeToken(func(context.Context, fosite.AccessRequester) error { before = true; return nil }),
		WithOnToken(func(context.Context, fosite.AccessRequester) error { after = true; return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	verifier := strings.Repeat("v", 64)
	code := authorize(t, first, verifier)
	status, tokens := tokenRequest(t, second, url.Values{"grant_type": {"authorization_code"}, "client_id": {"public"}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {"https://client.example/callback"}})
	if status != 200 || !before || !after {
		t.Fatal("persistent config or runtime token hooks failed", status, before, after)
	}
	if _, _, err := first.Provider.IntrospectToken(t.Context(), tokens["access_token"].(string), fosite.AccessToken, openid.NewDefaultSession()); err != nil {
		t.Fatal("loaded HMAC secret was not retained across construction", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*ServerConfig)
	}{
		{"insecure issuer", func(c *ServerConfig) { c.Issuer = "http://issuer.example" }},
		{"missing key ID", func(c *ServerConfig) { c.KeyID = "" }},
		{"missing key", func(c *ServerConfig) { c.SigningKeyFile = "" }},
		{"missing secret", func(c *ServerConfig) { c.HMACSecretFile = "" }},
		{"negative lifetime", func(c *ServerConfig) { c.CodeTTL = -time.Second }},
		{"zero lifetime", func(c *ServerConfig) { c.RefreshTokenTTL = 0 }},
		{"unreadable key", func(c *ServerConfig) { c.SigningKeyFile = filepath.Join(dir, "absent") }},
		{"unreadable rotation keys", func(c *ServerConfig) { c.PreviousJWKSFile = filepath.Join(dir, "absent") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := cfg
			tc.edit(&invalid)
			if _, err := NewServerFromConfig(invalid, store); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	if err := os.WriteFile(cfg.HMACSecretFile, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServerFromConfig(cfg, store); err == nil {
		t.Fatal("short HMAC secret accepted")
	}
}
