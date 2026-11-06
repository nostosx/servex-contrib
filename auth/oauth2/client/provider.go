package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	coreoidc "github.com/coreos/go-oidc/v3/oidc"
	sdk "golang.org/x/oauth2"

	"github.com/nostosx/servex-contrib/auth/oidc"
)

// ProviderKind selects a documented login protocol, not application permissions.
type ProviderKind string

const (
	Google    ProviderKind = "google"
	GitHub    ProviderKind = "github"
	Microsoft ProviderKind = "microsoft"
	GitLab    ProviderKind = "gitlab"
	Discord   ProviderKind = "discord"
	LinkedIn  ProviderKind = "linkedin"
	Apple     ProviderKind = "apple"
	OIDC      ProviderKind = "oidc"
)

// ProviderConfig is trusted administrator input. Issuer is required for OIDC
// and optional for self-hosted GitLab. Microsoft requires a concrete tenant UUID;
// common/organizations discovery templates are deliberately not accepted.
// Apple requires a current signed client-secret JWT and a form_post callback.
// HTTPClient is copied and redirects are rejected to prevent credential leakage.
type ProviderConfig struct {
	Kind                  ProviderKind
	ClientID              string
	ClientSecret          string
	RedirectURL           string
	Issuer                string
	Tenant                string
	Scopes                []string
	StateTTL              time.Duration
	HTTPClient            *http.Client
	AllowInsecureLoopback bool
}

// Identity contains authenticated provider claims. Use (Issuer, Subject) as the
// external identity key. Email is optional and is never an account-linking key.
// EmailVerified means the provider asserts mailbox verification, not legal identity.
type Identity struct {
	Provider      ProviderKind
	Issuer        string
	Subject       string
	Name          string
	Email         string
	EmailVerified bool
}

// Provider coordinates code exchange and identity verification. It owns no user
// accounts, sessions or HTTP routes. Reuse one instance per registered provider.
type Provider struct {
	kind     ProviderKind
	issuer   string
	flow     *Client
	verifier *oidc.Verifier
	http     *http.Client
}

// NewProvider discovers OIDC endpoints or selects official OAuth2 endpoints.
// Discovery and all subsequent backend requests have a finite timeout. Production
// callers must keep construction's context alive for the verifier's key cache.
func NewProvider(ctx context.Context, config ProviderConfig, store StateStore) (*Provider, error) {
	issuer, endpoint, scopes, err := providerDefaults(config)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: 10 * time.Second}
	if config.HTTPClient != nil {
		*httpClient = *config.HTTPClient
		if httpClient.Timeout <= 0 {
			httpClient.Timeout = 10 * time.Second
		}
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("oauth2: provider HTTP redirects are not allowed")
	}
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = boundedTransport{base: transport}
	p := &Provider{kind: config.Kind, issuer: issuer, http: httpClient}
	if endpoint.AuthURL == "" {
		if err := validateURL(issuer, config.AllowInsecureLoopback); err != nil {
			return nil, err
		}
		issuerURL, _ := url.Parse(issuer)
		if issuerURL.RawQuery != "" || issuerURL.ForceQuery {
			return nil, errors.New("oauth2: issuer must not contain a query")
		}
		// The SDK verifies discovery's exact issuer; an endpoint URL alone is
		// insufficient to establish trust in an OpenID provider.
		discovered, err := coreoidc.NewProvider(coreoidc.ClientContext(ctx, httpClient), issuer)
		if err != nil {
			return nil, fmt.Errorf("oauth2: provider discovery: %w", err)
		}
		endpoint = discovered.Endpoint()
		var metadata struct {
			JWKS string `json:"jwks_uri"`
		}
		if err := discovered.Claims(&metadata); err != nil {
			return nil, fmt.Errorf("oauth2: provider metadata: %w", err)
		}
		if err := validateURL(metadata.JWKS, config.AllowInsecureLoopback); err != nil {
			return nil, err
		}
		options := []oidc.Option{oidc.WithHTTPClient(httpClient)}
		if config.AllowInsecureLoopback {
			options = append(options, oidc.WithInsecureHTTP())
		}
		p.verifier, err = oidc.NewRemote(ctx, issuer, config.ClientID, metadata.JWKS, options...)
		if err != nil {
			return nil, err
		}
		// All built-in OIDC adapters are confidential backend clients. Sending
		// credentials explicitly avoids the SDK retrying an uncertain exchange.
		endpoint.AuthStyle = sdk.AuthStyleInParams
	}
	if len(config.Scopes) != 0 {
		scopes = slices.Clone(config.Scopes)
	}
	if p.verifier != nil && !slices.Contains(scopes, "openid") {
		return nil, errors.New("oauth2: OIDC login requires the openid scope")
	}
	p.flow, err = New(Config{OAuth: sdk.Config{ClientID: config.ClientID, ClientSecret: config.ClientSecret,
		RedirectURL: config.RedirectURL, Scopes: scopes, Endpoint: endpoint}, StateTTL: config.StateTTL,
		AllowInsecureLoopback: config.AllowInsecureLoopback}, store)
	if err != nil {
		return nil, err
	}
	if config.ClientSecret == "" {
		return nil, errors.New("oauth2: backend provider client secret required")
	}
	return p, nil
}

func providerDefaults(c ProviderConfig) (string, sdk.Endpoint, []string, error) {
	var issuer string
	var endpoint sdk.Endpoint
	scopes := []string{"openid", "profile", "email"}
	switch c.Kind {
	case Google:
		issuer = "https://accounts.google.com"
	case GitHub:
		issuer = "https://github.com"
		endpoint = sdk.Endpoint{AuthURL: issuer + "/login/oauth/authorize", TokenURL: issuer + "/login/oauth/access_token", AuthStyle: sdk.AuthStyleInParams}
		scopes = []string{"read:user", "user:email"}
	case Microsoft:
		if !regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).MatchString(c.Tenant) {
			return "", endpoint, nil, errors.New("oauth2: Microsoft requires a concrete tenant UUID")
		}
		issuer = "https://login.microsoftonline.com/" + strings.ToLower(c.Tenant) + "/v2.0"
	case GitLab:
		issuer = "https://gitlab.com"
		if c.Issuer != "" {
			issuer = c.Issuer
		}
	case Discord:
		issuer = "https://discord.com"
		endpoint = sdk.Endpoint{AuthURL: issuer + "/oauth2/authorize", TokenURL: issuer + "/api/oauth2/token", AuthStyle: sdk.AuthStyleInParams}
		scopes = []string{"identify", "email"}
	case LinkedIn:
		issuer = "https://www.linkedin.com/oauth"
	case Apple:
		issuer = "https://appleid.apple.com"
		scopes = []string{"openid", "email"}
	case OIDC:
		issuer = c.Issuer
		if issuer == "" {
			return "", endpoint, nil, errors.New("oauth2: OIDC issuer required")
		}
	default:
		return "", endpoint, nil, fmt.Errorf("oauth2: unknown provider %q", c.Kind)
	}
	if c.Issuer != "" && c.Issuer != issuer {
		return "", endpoint, nil, errors.New("oauth2: built-in provider issuer cannot be overridden")
	}
	return issuer, endpoint, scopes, nil
}

// Begin saves state/nonce/PKCE bound to this client and the caller's browser key.
// Apple uses response_mode=form_post; persist a Secure, SameSite=None binding
// cookie/session before redirecting. Other providers use a query callback.
func (p *Provider) Begin(ctx context.Context, binding string) (Authorization, error) {
	if binding == "" {
		return Authorization{}, ErrInvalidState
	}
	authorization, err := p.flow.Begin(ctx, p.binding(binding))
	if err != nil {
		return Authorization{}, err
	}
	if p.kind == Apple {
		u, _ := url.Parse(authorization.URL) // Constructed and validated by Client.
		query := u.Query()
		query.Set("response_mode", "form_post")
		u.RawQuery = query.Encode()
		authorization.URL = u.String()
	}
	return authorization, nil
}

// CallbackMethod is POST for Apple's form_post response and GET otherwise.
// POST handlers must parse a bounded application/x-www-form-urlencoded body;
// they must still validate state and the original browser/session binding.
func (p *Provider) CallbackMethod() string {
	if p.kind == Apple {
		return http.MethodPost
	}
	return http.MethodGet
}

// Complete consumes state before exchanging the code. OIDC requires a signed
// ID token with matching issuer, audience, expiry and nonce. Plain OAuth2 uses
// the provider's authenticated user API. Tokens are not exposed to the browser.
func (p *Provider) Complete(ctx context.Context, binding, state, code string) (Identity, error) {
	ctx = context.WithValue(ctx, sdk.HTTPClient, p.http)
	if binding == "" {
		return Identity{}, ErrInvalidState
	}
	result, err := p.flow.Complete(ctx, p.binding(binding), state, code)
	if err != nil {
		return Identity{}, err
	}
	if p.verifier == nil {
		return p.profile(ctx, result.Token)
	}
	raw, ok := result.Token.Extra("id_token").(string)
	if !ok || raw == "" {
		return Identity{}, errors.New("oauth2: provider omitted ID token")
	}
	token, err := p.verifier.VerifyNonce(ctx, raw, result.Nonce)
	if err != nil {
		return Identity{}, err
	}
	if token.AccessTokenHash != "" {
		if err := token.VerifyAccessToken(result.Token.AccessToken); err != nil {
			return Identity{}, fmt.Errorf("oauth2: access token hash: %w", err)
		}
	}
	var claims struct {
		Name          string          `json:"name"`
		Email         string          `json:"email"`
		EmailVerified json.RawMessage `json:"email_verified"`
	}
	if err := token.Claims(&claims); err != nil {
		return Identity{}, fmt.Errorf("oauth2: identity claims: %w", err)
	}
	verified := string(claims.EmailVerified) == "true" || p.kind == Apple && string(claims.EmailVerified) == `"true"`
	return Identity{Provider: p.kind, Issuer: p.issuer, Subject: token.Subject, Name: claims.Name,
		Email: claims.Email, EmailVerified: claims.Email != "" && verified}, nil
}

func (p *Provider) binding(browser string) string {
	return string(p.kind) + "\x00" + p.issuer + "\x00" + browser
}

// SDK discovery and key refresh also read remote documents. Bound those reads,
// including chunked responses, without buffering or changing caller transports.
type boundedTransport struct{ base http.RoundTripper }

func (t boundedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	response.Body = &boundedBody{ReadCloser: response.Body, remaining: 1 << 20}
	return response, nil
}

type boundedBody struct {
	io.ReadCloser
	remaining int
}

func (b *boundedBody) Read(buffer []byte) (int, error) {
	if b.remaining < 0 {
		return 0, errors.New("oauth2: provider response too large")
	}
	// Read one byte beyond the budget to distinguish an exact-length response
	// from an oversized one, even when Content-Length is absent or incorrect.
	if len(buffer) > b.remaining+1 {
		buffer = buffer[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(buffer)
	b.remaining -= n
	if b.remaining < 0 {
		return n, errors.New("oauth2: provider response too large")
	}
	return n, err
}

func (p *Provider) profile(ctx context.Context, token *sdk.Token) (Identity, error) {
	identity := Identity{Provider: p.kind, Issuer: p.issuer}
	switch p.kind {
	case GitHub:
		var user struct {
			ID    int64  `json:"id"`
			Name  string `json:"name"`
			Login string `json:"login"`
		}
		if err := p.getJSON(ctx, "https://api.github.com/user", token, &user); err != nil {
			return Identity{}, err
		}
		if user.ID <= 0 {
			return Identity{}, errors.New("oauth2: GitHub omitted user ID")
		}
		identity.Subject, identity.Name = strconv.FormatInt(user.ID, 10), user.Name
		if identity.Name == "" {
			identity.Name = user.Login
		}
		// The public profile's email field has no verification flag. Only
		// authenticated /user/emails records may assert mailbox verification.
		var emails []struct {
			Email    string `json:"email"`
			Verified bool   `json:"verified"`
			Primary  bool   `json:"primary"`
		}
		if slices.Contains(p.flow.config.Scopes, "user:email") {
			if err := p.getJSON(ctx, "https://api.github.com/user/emails?per_page=100", token, &emails); err != nil {
				return Identity{}, err
			}
			for _, email := range emails {
				if email.Verified && email.Email != "" && (identity.Email == "" || email.Primary) {
					identity.Email, identity.EmailVerified = email.Email, true
				}
			}
		}
	case Discord:
		var user struct {
			ID       string `json:"id"`
			Name     string `json:"global_name"`
			Username string `json:"username"`
			Email    string `json:"email"`
			Verified bool   `json:"verified"`
		}
		if err := p.getJSON(ctx, "https://discord.com/api/v10/users/@me", token, &user); err != nil {
			return Identity{}, err
		}
		if _, err := strconv.ParseUint(user.ID, 10, 64); err != nil || user.ID == "0" {
			return Identity{}, errors.New("oauth2: Discord omitted user ID")
		}
		identity.Subject, identity.Name = user.ID, user.Name
		if identity.Name == "" {
			identity.Name = user.Username
		}
		identity.Email, identity.EmailVerified = user.Email, user.Email != "" && user.Verified
	default:
		return Identity{}, errors.New("oauth2: provider identity adapter missing")
	}
	return identity, nil
}

func (p *Provider) getJSON(ctx context.Context, endpoint string, token *sdk.Token, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token.AccessToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "nostosx-oauth2")
	response, err := p.http.Do(request)
	if err != nil {
		return fmt.Errorf("oauth2: provider user API: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("oauth2: provider user API returned HTTP %d", response.StatusCode)
	}
	const maxBody = 1 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil {
		return fmt.Errorf("oauth2: read provider identity: %w", err)
	}
	if len(body) > maxBody {
		return errors.New("oauth2: provider identity response too large")
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("oauth2: decode provider identity: %w", err)
	}
	return nil
}
