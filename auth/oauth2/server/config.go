package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-jose/go-jose/v3"
	"github.com/ory/fosite"
)

// ServerConfig contains file-decodable provider settings. Start with
// DefaultServerConfig before decoding. Application scopes, enablement and
// account/consent policy do not belong to the protocol configuration.
type ServerConfig struct {
	Issuer                string        `json:"issuer"`
	KeyID                 string        `json:"key_id"`
	SigningKeyFile        string        `json:"signing_key_file"`
	HMACSecretFile        string        `json:"hmac_secret_file"`
	PreviousJWKSFile      string        `json:"previous_jwks_file"`
	AllowInsecureLoopback bool          `json:"allow_insecure_loopback"`
	AccessTokenTTL        time.Duration `json:"access_token_ttl"`
	RefreshTokenTTL       time.Duration `json:"refresh_token_ttl"`
	CodeTTL               time.Duration `json:"code_ttl"`
}

// DefaultServerConfig supplies protocol lifetimes; issuer and keys are explicit.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: 7 * 24 * time.Hour, CodeTTL: 2 * time.Minute}
}

// Validate checks scalar configuration without reading files. Construction
// validates the loaded RSA key, HMAC secret and rotation keys as well.
func (c ServerConfig) Validate() error {
	if err := validateIssuer(c.Issuer, c.AllowInsecureLoopback); err != nil {
		return err
	}
	if c.KeyID == "" || c.SigningKeyFile == "" || c.HMACSecretFile == "" {
		return errors.New("oauth2 server: key ID, signing key file and HMAC secret file required")
	}
	if c.AccessTokenTTL <= 0 || c.RefreshTokenTTL <= 0 || c.CodeTTL <= 0 {
		return errors.New("oauth2 server: credential lifetimes must be positive")
	}
	return nil
}

// ServerOption supplies runtime dependencies independently of decoded settings.
type ServerOption func(*serverOptions)

type serverOptions struct {
	hasher      fosite.Hasher
	beforeToken func(context.Context, fosite.AccessRequester) error
	onToken     func(context.Context, fosite.AccessRequester) error
}

// WithClientSecretsHasher replaces Fosite's default client-secret hasher.
func WithClientSecretsHasher(hasher fosite.Hasher) ServerOption {
	return func(o *serverOptions) { o.hasher = hasher }
}

// WithBeforeToken installs application policy before issuing token claims.
func WithBeforeToken(fn func(context.Context, fosite.AccessRequester) error) ServerOption {
	return func(o *serverOptions) { o.beforeToken = fn }
}

// WithOnToken installs an application hook after token response construction.
func WithOnToken(fn func(context.Context, fosite.AccessRequester) error) ServerOption {
	return func(o *serverOptions) { o.onToken = fn }
}

// NewServerFromConfig loads persistent keys and constructs the provider without
// opening listeners. Secret files contain raw bytes, not trimmed text. The server
// retains its own HMAC secret for its lifetime; replicas must share these files.
func NewServerFromConfig(cfg ServerConfig, store Store, opts ...ServerOption) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := serverOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	key, err := LoadRSAKey(cfg.SigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("oauth2 server: load signing key: %w", err)
	}
	secret, err := os.ReadFile(cfg.HMACSecretFile)
	if err != nil {
		return nil, fmt.Errorf("oauth2 server: load HMAC secret: %w", err)
	}
	defer clear(secret)
	var previous jose.JSONWebKeySet
	if cfg.PreviousJWKSFile != "" {
		data, err := os.ReadFile(cfg.PreviousJWKSFile)
		if err != nil {
			return nil, fmt.Errorf("oauth2 server: load previous JWKS: %w", err)
		}
		if err := json.Unmarshal(data, &previous); err != nil {
			return nil, fmt.Errorf("oauth2 server: decode previous JWKS: %w", err)
		}
	}
	ownedSecret := append([]byte(nil), secret...)
	provider, err := New(Config{
		Issuer: cfg.Issuer, Key: key, KeyID: cfg.KeyID, PublicKeys: previous.Keys,
		AllowInsecureLoopback: cfg.AllowInsecureLoopback,
		OAuth: fosite.Config{GlobalSecret: ownedSecret, AccessTokenLifespan: cfg.AccessTokenTTL,
			RefreshTokenLifespan: cfg.RefreshTokenTTL, AuthorizeCodeLifespan: cfg.CodeTTL,
			IDTokenLifespan: cfg.AccessTokenTTL, ClientSecretsHasher: o.hasher},
		BeforeToken: o.beforeToken, OnToken: o.onToken,
	}, store)
	if err != nil {
		clear(ownedSecret)
	}
	return provider, err
}
