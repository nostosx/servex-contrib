// Package oauth2 coordinates authorization-code login using Go's OAuth2 SDK,
// PKCE, one-time state and an application-supplied browser/session binding.
package oauth2

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	sdk "golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var ErrInvalidState = errors.New("oauth2: invalid, expired or consumed login state")

// Attempt is sensitive login state. Persist it server-side, never in a URL or
// plaintext browser cookie. Binding is a SHA-256 digest of a trusted browser key.
type Attempt struct {
	Verifier  string
	Nonce     string
	Binding   [32]byte
	ExpiresAt time.Time
}

// StateStore must be shared across replicas. Save is insert-only and Take must
// atomically remove and return a live attempt. Honor ctx; never log state data.
type StateStore interface {
	Save(ctx context.Context, key string, attempt Attempt) error
	Take(ctx context.Context, key string) (Attempt, error)
}

// Config requires HTTPS endpoints and redirect URI. AllowInsecureLoopback only
// permits HTTP on localhost or loopback IP literals, for local development.
type Config struct {
	OAuth                 sdk.Config
	StateTTL              time.Duration
	AllowInsecureLoopback bool
}

// Client is immutable. Configure a finite-timeout HTTP client in the operation
// context with oauth2.HTTPClient. SDK config, scopes and endpoints are snapshotted.
type Client struct {
	config sdk.Config
	store  StateStore
	ttl    time.Duration
}

func New(config Config, store StateStore) (*Client, error) {
	if store == nil || strings.TrimSpace(config.OAuth.ClientID) == "" {
		return nil, errors.New("oauth2: client ID and state store required")
	}
	if config.StateTTL == 0 {
		config.StateTTL = 5 * time.Minute
	}
	if config.StateTTL < time.Second || config.StateTTL > 10*time.Minute {
		return nil, errors.New("oauth2: state TTL must be one second to ten minutes")
	}
	for _, endpoint := range []string{config.OAuth.Endpoint.AuthURL, config.OAuth.Endpoint.TokenURL, config.OAuth.RedirectURL} {
		if err := validateURL(endpoint, config.AllowInsecureLoopback); err != nil {
			return nil, err
		}
	}
	copyConfig := config.OAuth
	copyConfig.Scopes = slices.Clone(copyConfig.Scopes)
	return &Client{config: copyConfig, store: store, ttl: config.StateTTL}, nil
}

// Authorization contains the public redirect URL only. The verifier and nonce
// remain in StateStore. The application performs the HTTP redirect after its
// session/browser binding has been persisted successfully.
type Authorization struct {
	URL       string
	ExpiresAt time.Time
}

func (c *Client) Begin(ctx context.Context, binding string) (Authorization, error) {
	if err := context.Cause(ctx); err != nil {
		return Authorization{}, err
	}
	if binding == "" {
		return Authorization{}, errors.New("oauth2: trusted browser binding required")
	}
	state := rand.Text()
	attempt := Attempt{Verifier: sdk.GenerateVerifier(), Nonce: rand.Text(),
		Binding: sha256.Sum256([]byte(binding)), ExpiresAt: time.Now().Add(c.ttl)}
	if err := c.store.Save(ctx, stateKey(state), attempt); err != nil {
		return Authorization{}, fmt.Errorf("oauth2: save state: %w", err)
	}
	redirect := c.config.AuthCodeURL(state, sdk.S256ChallengeOption(attempt.Verifier), sdk.SetAuthURLParam("nonce", attempt.Nonce))
	return Authorization{URL: redirect, ExpiresAt: attempt.ExpiresAt}, nil
}

// Result must still pass OIDC ID-token verification when used for authentication.
// Verify the ID token with the returned Nonce; a token exchange alone does not
// establish the user's identity or grant application permissions.
type Result struct {
	Token *sdk.Token
	Nonce string
}

// Complete consumes state before exchanging code. Failed exchanges cannot be
// retried with the same state; start a fresh login after an uncertain response.
func (c *Client) Complete(ctx context.Context, binding, state, code string) (Result, error) {
	if err := context.Cause(ctx); err != nil {
		return Result{}, err
	}
	if binding == "" || state == "" || len(state) > 256 || code == "" {
		return Result{}, ErrInvalidState
	}
	attempt, err := c.store.Take(ctx, stateKey(state))
	if err != nil {
		return Result{}, fmt.Errorf("oauth2: consume state: %w", err)
	}
	actual := sha256.Sum256([]byte(binding))
	if subtle.ConstantTimeCompare(actual[:], attempt.Binding[:]) != 1 {
		return Result{}, ErrInvalidState
	}
	if !time.Now().Before(attempt.ExpiresAt) || attempt.Verifier == "" || attempt.Nonce == "" {
		return Result{}, ErrInvalidState
	}
	token, err := c.config.Exchange(ctx, code, sdk.VerifierOption(attempt.Verifier))
	if err != nil {
		return Result{}, fmt.Errorf("oauth2: exchange: %w", err)
	}
	return Result{Token: token, Nonce: attempt.Nonce}, nil
}

// TokenSource uses SDK refresh behavior. Its context must outlive refreshes;
// store refreshed tokens explicitly if they must survive application restarts.
func (c *Client) TokenSource(ctx context.Context, token *sdk.Token) sdk.TokenSource {
	return c.config.TokenSource(ctx, token)
}

// ClientCredentials returns the official SDK's client-credentials token source.
// The application chooses scopes and owns the source's context lifetime.
func ClientCredentials(ctx context.Context, config clientcredentials.Config) (sdk.TokenSource, error) {
	if config.ClientID == "" || config.ClientSecret == "" {
		return nil, errors.New("oauth2: client credentials required")
	}
	if err := validateURL(config.TokenURL, false); err != nil {
		return nil, err
	}
	config.Scopes = slices.Clone(config.Scopes)
	config.EndpointParams = config.EndpointParams.Clone()
	return config.TokenSource(ctx), nil
}

func stateKey(state string) string {
	digest := sha256.Sum256([]byte(state))
	return hex.EncodeToString(digest[:])
}

func validateURL(raw string, allowLoopback bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("oauth2: invalid endpoint or redirect URL")
	}
	if u.Scheme == "https" {
		return nil
	}
	if allowLoopback && u.Scheme == "http" && loopback(u.Hostname()) {
		return nil
	}
	return errors.New("oauth2: endpoint and redirect URLs require HTTPS")
}
