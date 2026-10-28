// Package oidc verifies OpenID Connect ID tokens using provider discovery or
// an explicitly configured JWKS URL. Verify can be passed to servex auth.New.
package oidc

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	coreoidc "github.com/coreos/go-oidc/v3/oidc"
)

// IDToken contains verified standard claims. Its Claims method decodes custom
// claims. The alias retains the SDK's access-token-hash verification method.
type IDToken = coreoidc.IDToken

// Option configures a Verifier. Signature, issuer, audience and expiry checks
// cannot be disabled.
type Option func(*options)

type options struct {
	httpClient *http.Client
	algorithms []string
	insecure   bool
}

// WithHTTPClient selects the HTTP client used for discovery and JWKS refresh.
// The client is copied; a missing timeout defaults to ten seconds. Its existing
// redirect policy is retained inside the HTTPS and ten-redirect restrictions.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) { o.httpClient = client }
}

// WithSigningAlgorithms replaces the default RS256 allowlist. Only asymmetric
// RSA, ECDSA and EdDSA algorithms are accepted; "none" and HMAC are rejected.
func WithSigningAlgorithms(algorithms ...string) Option {
	return func(o *options) { o.algorithms = slices.Clone(algorithms) }
}

// WithInsecureHTTP permits cleartext discovery and JWKS URLs. Use only with
// trusted local test providers; production providers must use HTTPS.
func WithInsecureHTTP() Option { return func(o *options) { o.insecure = true } }

// Verifier is a reusable, concurrency-safe verifier with cached signing keys.
// Create one per trusted issuer and expected audience, not one per request.
type Verifier struct {
	verifier *coreoidc.IDTokenVerifier
	issuer   string
	audience string
}

// New discovers the provider's JWKS URL and verifies that its declared issuer
// matches issuer. Construction is bounded by ctx and the HTTP client timeout.
func New(ctx context.Context, issuer, audience string, opts ...Option) (*Verifier, error) {
	o, client, err := configure(issuer, audience, opts)
	if err != nil {
		return nil, err
	}
	ctx = coreoidc.ClientContext(ctx, client)
	provider, err := coreoidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("oidc: discover provider: %w", err)
	}
	var metadata struct {
		JWKS string `json:"jwks_uri"`
	}
	if err := provider.Claims(&metadata); err != nil {
		return nil, fmt.Errorf("oidc: decode provider metadata: %w", err)
	}
	if err := checkURL(metadata.JWKS, o.insecure); err != nil {
		return nil, fmt.Errorf("oidc: invalid jwks url: %w", err)
	}
	return &Verifier{verifier: provider.VerifierContext(ctx, &coreoidc.Config{ClientID: audience, SupportedSigningAlgs: o.algorithms}), issuer: issuer, audience: audience}, nil
}

// NewRemote uses a trusted, explicit JWKS URL instead of provider discovery.
// It performs no HTTP request until the first token needs a signing key.
func NewRemote(ctx context.Context, issuer, audience, jwksURL string, opts ...Option) (*Verifier, error) {
	o, client, err := configure(issuer, audience, opts)
	if err != nil {
		return nil, err
	}
	if err := checkURL(jwksURL, o.insecure); err != nil {
		return nil, fmt.Errorf("oidc: invalid jwks url: %w", err)
	}
	keys := coreoidc.NewRemoteKeySet(coreoidc.ClientContext(ctx, client), jwksURL)
	return &Verifier{verifier: coreoidc.NewVerifier(issuer, keys, &coreoidc.Config{ClientID: audience, SupportedSigningAlgs: o.algorithms}), issuer: issuer, audience: audience}, nil
}

// Verify checks signature, exact issuer, audience, expiry and subject. It also
// checks azp when present, and requires it when the token has multiple
// audiences. The SDK validates nbf with its documented five-minute leeway.
// Use VerifyNonce for ID tokens received during an OIDC login flow.
func (v *Verifier) Verify(ctx context.Context, raw string) (*IDToken, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("oidc: verify id token: %w", err)
	}
	if token.Issuer != v.issuer || token.Subject == "" {
		return nil, errors.New("oidc: invalid issuer or missing subject")
	}
	var claims struct {
		AuthorizedParty string `json:"azp"`
	}
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("oidc: decode authorized party: %w", err)
	}
	if (claims.AuthorizedParty != "" || len(token.Audience) > 1) && claims.AuthorizedParty != v.audience {
		return nil, errors.New("oidc: invalid authorized party")
	}
	return token, nil
}

// VerifyNonce verifies the token and compares its nonce with the non-empty
// nonce saved for this login attempt. The application owns nonce generation,
// session binding and one-time consumption, and must also validate OAuth state.
func (v *Verifier) VerifyNonce(ctx context.Context, raw, nonce string) (*IDToken, error) {
	if nonce == "" {
		return nil, errors.New("oidc: expected nonce is required")
	}
	token, err := v.Verify(ctx, raw)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(token.Nonce), []byte(nonce)) != 1 {
		return nil, errors.New("oidc: invalid nonce")
	}
	return token, nil
}

func configure(issuer, audience string, opts []Option) (options, *http.Client, error) {
	o := options{algorithms: []string{"RS256"}}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if err := checkURL(issuer, o.insecure); err != nil {
		return o, nil, fmt.Errorf("oidc: invalid issuer url: %w", err)
	}
	issuerURL, _ := url.Parse(issuer) // checkURL already parsed it.
	if issuerURL.RawQuery != "" || issuerURL.ForceQuery || strings.TrimSpace(audience) == "" {
		return o, nil, errors.New("oidc: issuer must have no query and audience must be non-empty")
	}
	if len(o.algorithms) == 0 {
		return o, nil, errors.New("oidc: at least one signing algorithm is required")
	}
	for _, algorithm := range o.algorithms {
		switch algorithm {
		case "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA":
		default:
			return o, nil, fmt.Errorf("oidc: unsupported signing algorithm %q", algorithm)
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if o.httpClient != nil {
		*client = *o.httpClient
		if client.Timeout <= 0 {
			client.Timeout = 10 * time.Second
		}
	}
	redirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("oidc: too many redirects")
		}
		if err := checkURL(request.URL.String(), o.insecure); err != nil {
			return err
		}
		if redirect != nil {
			return redirect(request, via)
		}
		return nil
	}
	return o, client, nil
}

func checkURL(raw string, insecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	schemeAllowed := u.Scheme == "https" || insecure && u.Scheme == "http"
	if u.Host == "" || u.User != nil || u.Fragment != "" || !schemeAllowed {
		return errors.New("an absolute https url without userinfo or fragment is required")
	}
	return nil
}
