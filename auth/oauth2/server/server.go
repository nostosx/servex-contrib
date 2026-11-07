// Package server adapts Fosite's authorization-code/OIDC provider.
// Applications own authentication, consent, client policy and durable storage.
package server

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-jose/go-jose/v3"
	"github.com/ory/fosite"
	"github.com/ory/fosite/compose"
	"github.com/ory/fosite/handler/oauth2"
	"github.com/ory/fosite/handler/openid"
	"github.com/ory/fosite/handler/pkce"
	"github.com/ory/fosite/token/jwt"
)

// Store is the storage required by the enabled protocol handlers.
type Store interface {
	fosite.Storage
	oauth2.CoreStorage
	oauth2.TokenRevocationStorage
	openid.OpenIDConnectRequestStorage
	pkce.PKCERequestStorage
}

// Config uses native SDK policy. Keys must be persistent and shared by replicas.
// PublicKeys can retain previous public keys during signing-key rotation.
type Config struct {
	OAuth                 fosite.Config
	Issuer                string
	Key                   *rsa.PrivateKey
	KeyID                 string
	PublicKeys            []jose.JSONWebKey
	AllowInsecureLoopback bool
	OnToken               func(context.Context, fosite.AccessRequester) error
	BeforeToken           func(context.Context, fosite.AccessRequester) error
}

// Server exposes the native provider for application-owned authorization policy.
type Server struct {
	Provider    fosite.OAuth2Provider
	issuer      string
	keys        jose.JSONWebKeySet
	onToken     func(context.Context, fosite.AccessRequester) error
	beforeToken func(context.Context, fosite.AccessRequester) error
}

func New(config Config, store Store) (*Server, error) {
	if err := validateIssuer(config.Issuer, config.AllowInsecureLoopback); err != nil {
		return nil, err
	}
	if store == nil || config.Key == nil || config.Key.N == nil || config.Key.N.BitLen() < 2048 || config.KeyID == "" || len(config.OAuth.GlobalSecret) < 32 {
		return nil, errors.New("oauth2 server: store, RSA key (2048+ bits), key ID and 32-byte global secret required")
	}
	if err := config.Key.Validate(); err != nil {
		return nil, err
	}
	cfg := config.OAuth
	if cfg.AccessTokenLifespan < 0 || cfg.RefreshTokenLifespan < 0 || cfg.AuthorizeCodeLifespan < 0 || cfg.IDTokenLifespan < 0 {
		return nil, errors.New("oauth2 server: credential lifetimes must be finite")
	}
	cfg.IDTokenIssuer, cfg.TokenURL = config.Issuer, config.Issuer+"/oauth2/token"
	cfg.EnforcePKCE, cfg.EnablePKCEPlainChallengeMethod = true, false
	cfg.SendDebugMessagesToClients = false
	cfg.ScopeStrategy, cfg.AudienceMatchingStrategy = fosite.ExactScopeStrategy, fosite.ExactAudienceMatchingStrategy
	cfg.RefreshTokenScopes = []string{"offline_access"}
	// SDK getters lazily assign these defaults. Initialize them before serving
	// concurrent requests so first-time client authentication cannot race.
	_ = cfg.GetSecretsHasher(context.Background())
	_ = cfg.GetJWKSFetcherStrategy(context.Background())
	key := &jose.JSONWebKey{Key: config.Key, KeyID: config.KeyID, Algorithm: "RS256", Use: "sig"}
	keyGetter := func(context.Context) (any, error) { return key, nil }
	strategy := &compose.CommonStrategy{
		CoreStrategy:               compose.NewOAuth2HMACStrategy(&cfg),
		OpenIDConnectTokenStrategy: compose.NewOpenIDConnectStrategy(keyGetter, &cfg),
		Signer:                     &jwt.DefaultSigner{GetPrivateKey: keyGetter},
	}
	provider := compose.Compose(&cfg, store, strategy,
		compose.OAuth2AuthorizeExplicitFactory,
		compose.OAuth2ClientCredentialsGrantFactory,
		compose.OAuth2RefreshTokenGrantFactory,
		compose.OpenIDConnectExplicitFactory,
		compose.OpenIDConnectRefreshFactory,
		compose.OAuth2TokenIntrospectionFactory,
		compose.OAuth2TokenRevocationFactory,
		compose.OAuth2PKCEFactory,
	)
	keys := []jose.JSONWebKey{key.Public()}
	for _, previous := range config.PublicKeys {
		if previous.KeyID == "" || !previous.Valid() || !previous.IsPublic() {
			return nil, errors.New("oauth2 server: rotation keys must be valid public JWKs with key IDs")
		}
		if previous.KeyID != config.KeyID {
			keys = append(keys, previous)
		}
	}
	return &Server{Provider: provider, issuer: config.Issuer, keys: jose.JSONWebKeySet{Keys: keys}, onToken: config.OnToken, beforeToken: config.BeforeToken}, nil
}

func (s *Server) Discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"issuer":                                s.issuer,
		"authorization_endpoint":                s.issuer + "/oauth2/authorize",
		"token_endpoint":                        s.issuer + "/oauth2/token",
		"userinfo_endpoint":                     s.issuer + "/oauth2/userinfo",
		"jwks_uri":                              s.issuer + "/oauth2/jwks",
		"revocation_endpoint":                   s.issuer + "/oauth2/revoke",
		"introspection_endpoint":                s.issuer + "/oauth2/introspect",
		"response_types_supported":              []string{"code"},
		"response_modes_supported":              []string{"query"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token", "client_credentials"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post", "none"},
		"code_challenge_methods_supported":      []string{"S256"},
		"claims_supported":                      []string{"sub", "iss", "aud", "exp", "iat", "auth_time", "nonce", "name", "email", "email_verified"},
	})
}

func (s *Server) JWKS(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.keys) }

func (s *Server) Token(w http.ResponseWriter, r *http.Request) {
	request, err := s.Provider.NewAccessRequest(r.Context(), r, openid.NewDefaultSession())
	if err != nil {
		s.Provider.WriteAccessError(r.Context(), w, request, err)
		return
	}
	if request.GetGrantTypes().ExactOne("client_credentials") {
		if request.GetRequestedScopes().HasOneOf("openid", "offline_access") {
			s.Provider.WriteAccessError(r.Context(), w, request, fosite.ErrInvalidScope)
			return
		}
		for _, scope := range request.GetRequestedScopes() {
			request.GrantScope(scope)
		}
		for _, audience := range request.GetRequestedAudience() {
			request.GrantAudience(audience)
		}
		request.GetSession().(*openid.DefaultSession).Subject = request.GetClient().GetID()
	}
	if request.GetGrantTypes().ExactOne("refresh_token") && r.PostForm.Get("scope") != "" {
		scopes := strings.Fields(r.PostForm.Get("scope"))
		for _, scope := range scopes {
			if !request.GetGrantedScopes().Has(scope) {
				s.Provider.WriteAccessError(r.Context(), w, request, fosite.ErrInvalidScope)
				return
			}
		}
		// Fosite v0.49 restores the original grant during refresh. Apply the
		// caller's narrower scope after SDK validation and before persistence.
		native, ok := request.(*fosite.AccessRequest)
		if !ok {
			s.Provider.WriteAccessError(r.Context(), w, request, fosite.ErrServerError)
			return
		}
		native.GrantedScope = nil
		for _, scope := range scopes {
			request.GrantScope(scope)
		}
		request.SetRequestedScopes(scopes)
	}
	if s.beforeToken != nil {
		if err := s.beforeToken(r.Context(), request); err != nil {
			s.Provider.WriteAccessError(r.Context(), w, request, err)
			return
		}
	}
	response, err := s.Provider.NewAccessResponse(r.Context(), request)
	if err != nil {
		s.Provider.WriteAccessError(r.Context(), w, request, err)
		return
	}
	if request.GetGrantTypes().ExactOne("refresh_token") && !request.GetGrantedScopes().Has("offline_access") {
		if native, ok := response.(*fosite.AccessResponse); ok {
			delete(native.Extra, "refresh_token")
		}
	}
	if s.onToken != nil {
		if err := s.onToken(r.Context(), request); err != nil {
			s.Provider.WriteAccessError(r.Context(), w, request, fosite.ErrServerError)
			return
		}
	}
	s.Provider.WriteAccessResponse(r.Context(), w, request, response)
}

func (s *Server) Revoke(w http.ResponseWriter, r *http.Request) {
	err := s.Provider.NewRevocationRequest(r.Context(), r)
	s.Provider.WriteRevocationResponse(r.Context(), w, err)
}

func (s *Server) Introspect(w http.ResponseWriter, r *http.Request) {
	id, _, ok := r.BasicAuth()
	if !ok {
		s.Provider.WriteIntrospectionError(r.Context(), w, fosite.ErrRequestUnauthorized)
		return
	}
	clientID, err := url.QueryUnescape(id)
	if err != nil {
		s.Provider.WriteIntrospectionError(r.Context(), w, fosite.ErrRequestUnauthorized)
		return
	}
	response, err := s.Provider.NewIntrospectionRequest(r.Context(), r, openid.NewDefaultSession())
	if err != nil {
		s.Provider.WriteIntrospectionError(r.Context(), w, err)
		return
	}
	if response.IsActive() {
		request := response.GetAccessRequester()
		allowed := request.GetClient().GetID() == clientID
		for _, audience := range request.GetGrantedAudience() {
			if audience == clientID {
				allowed = true
			}
		}
		if !allowed {
			response = &fosite.IntrospectionResponse{Active: false}
		}
	}
	s.Provider.WriteIntrospectionResponse(r.Context(), w, response)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

func validateIssuer(issuer string, allowInsecureLoopback bool) error {
	u, err := url.Parse(issuer)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(issuer, "/") {
		return errors.New("oauth2 server: issuer must be an absolute URL without trailing slash, query or fragment")
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	localHTTP := allowInsecureLoopback && u.Scheme == "http" && local
	if u.Scheme != "https" && !localHTTP {
		return errors.New("oauth2 server: HTTPS issuer required")
	}
	return nil
}
