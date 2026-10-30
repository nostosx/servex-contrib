package jwt_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sdkjwt "github.com/golang-jwt/jwt/v5"

	"github.com/nostosx/servex-contrib/auth/jwt"
	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

type claims struct {
	sdkjwt.RegisteredClaims
	Role string `json:"role"`
}

var errRole = errors.New("unexpected role")

func (c *claims) Validate() error {
	if c.Role != "reader" {
		return errRole
	}
	return nil
}

func validClaims() *claims {
	return &claims{
		Issuer: "issuer", Subject: "user-1", Audience: sdkjwt.ClaimStrings{"api"},
		ExpiresAt: sdkjwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt:  sdkjwt.NewNumericDate(time.Now().Add(-time.Minute)),
		Role:      "reader",
	}
}

func config(algorithm string, key any) jwt.VerifierConfig {
	return jwt.VerifierConfig{
		Algorithm: algorithm, Issuer: "issuer", Audience: "api",
		KeyFunc: func(context.Context, *sdkjwt.Token) (any, error) { return key, nil },
	}
}

func verifier(t *testing.T, cfg jwt.VerifierConfig) *jwt.Verifier[*claims] {
	t.Helper()
	v, err := jwt.NewVerifier(cfg, func() *claims { return &claims{} })
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func sign(t *testing.T, algorithm string, key any, c sdkjwt.Claims) string {
	t.Helper()
	s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: algorithm, Key: key, KeyID: "current"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.Sign(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSigningAlgorithms(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	edPublic, edPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, algorithm := range []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"} {
		t.Run(algorithm, func(t *testing.T) {
			var private, public any
			switch {
			case strings.HasPrefix(algorithm, "HS"):
				private = []byte(strings.Repeat("s", 64))
				public = private
			case strings.HasPrefix(algorithm, "RS"), strings.HasPrefix(algorithm, "PS"):
				private, public = rsaKey, &rsaKey.PublicKey
			case strings.HasPrefix(algorithm, "ES"):
				curve := map[string]elliptic.Curve{"ES256": elliptic.P256(), "ES384": elliptic.P384(), "ES512": elliptic.P521()}[algorithm]
				key, err := ecdsa.GenerateKey(curve, rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				private, public = key, &key.PublicKey
			default:
				private, public = edPrivate, edPublic
			}
			cfg := config(algorithm, public)
			cfg.KeyFunc = func(_ context.Context, token *sdkjwt.Token) (any, error) {
				if token.Header["kid"] != "current" {
					return nil, errors.New("unknown key")
				}
				return public, nil
			}
			got, err := verifier(t, cfg).Verify(t.Context(), sign(t, algorithm, private, validClaims()))
			if err != nil || got.Subject != "user-1" || got.Role != "reader" {
				t.Fatalf("claims = %v, error = %v", got, err)
			}
		})
	}
}

func TestRejectInvalidClaims(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	v := verifier(t, config("HS256", key))
	for _, tc := range []struct {
		name   string
		change func(*claims)
		want   error
	}{
		{"issuer", func(c *claims) { c.Issuer = "other" }, sdkjwt.ErrTokenInvalidIssuer},
		{"missing issuer", func(c *claims) { c.Issuer = "" }, sdkjwt.ErrTokenRequiredClaimMissing},
		{"audience", func(c *claims) { c.Audience = sdkjwt.ClaimStrings{"other"} }, sdkjwt.ErrTokenInvalidAudience},
		{"missing audience", func(c *claims) { c.Audience = nil }, sdkjwt.ErrTokenRequiredClaimMissing},
		{"expired", func(c *claims) { c.ExpiresAt = sdkjwt.NewNumericDate(time.Now().Add(-time.Hour)) }, sdkjwt.ErrTokenExpired},
		{"missing expiry", func(c *claims) { c.ExpiresAt = nil }, sdkjwt.ErrTokenRequiredClaimMissing},
		{"future not before", func(c *claims) { c.NotBefore = sdkjwt.NewNumericDate(time.Now().Add(time.Hour)) }, sdkjwt.ErrTokenNotValidYet},
		{"future issued at", func(c *claims) { c.IssuedAt = sdkjwt.NewNumericDate(time.Now().Add(time.Hour)) }, sdkjwt.ErrTokenUsedBeforeIssued},
		{"missing subject", func(c *claims) { c.Subject = "" }, sdkjwt.ErrTokenRequiredClaimMissing},
		{"blank subject", func(c *claims) { c.Subject = " " }, sdkjwt.ErrTokenRequiredClaimMissing},
		{"custom validation", func(c *claims) { c.Role = "admin" }, errRole},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := validClaims()
			tc.change(c)
			got, err := v.Verify(t.Context(), sign(t, "HS256", key, c))
			if got != nil || !errors.Is(err, tc.want) {
				t.Fatalf("claims = %v, error = %v; want %v", got, err, tc.want)
			}
		})
	}
	c := validClaims()
	c.Audience = sdkjwt.ClaimStrings{"other", "api"}
	if _, err := v.Verify(t.Context(), sign(t, "HS256", key, c)); err != nil {
		t.Fatalf("multiple audiences: %v", err)
	}
}

func TestRejectTamperingAndAlgorithms(t *testing.T) {
	key := []byte(strings.Repeat("s", 64))
	lookupCalls := 0
	cfg := config("HS256", key)
	cfg.KeyFunc = func(context.Context, *sdkjwt.Token) (any, error) {
		lookupCalls++
		return key, nil
	}
	v := verifier(t, cfg)
	raw := sign(t, "HS256", key, validClaims())
	parts := strings.Split(raw, ".")
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"attacker"}`))
	if got, err := v.Verify(t.Context(), strings.Join(parts, ".")); got != nil || !errors.Is(err, sdkjwt.ErrTokenSignatureInvalid) {
		t.Fatalf("tampering: %v, %v", got, err)
	}
	unsigned, err := sdkjwt.NewWithClaims(sdkjwt.SigningMethodNone, validClaims()).SignedString(sdkjwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{unsigned, sign(t, "HS512", key, validClaims()), "bad", "a.b.c.d", ""} {
		before := lookupCalls
		if got, err := v.Verify(t.Context(), raw); got != nil || err == nil {
			t.Fatalf("invalid token accepted: %v, %v", got, err)
		}
		if lookupCalls != before {
			t.Fatal("key lookup invoked before algorithm/format rejection")
		}
	}
	wrongKey := []byte(strings.Repeat("x", 64))
	if _, err := v.Verify(t.Context(), sign(t, "HS256", wrongKey, validClaims())); !errors.Is(err, sdkjwt.ErrTokenSignatureInvalid) {
		t.Fatalf("wrong key: %v", err)
	}
}

func TestConfigurationAndClaimsFactory(t *testing.T) {
	key := []byte(strings.Repeat("s", 64))
	for _, change := range []func(*jwt.VerifierConfig){
		func(c *jwt.VerifierConfig) { c.Algorithm = "" },
		func(c *jwt.VerifierConfig) { c.Algorithm = "none" },
		func(c *jwt.VerifierConfig) { c.Algorithm = "unknown" },
		func(c *jwt.VerifierConfig) { c.Issuer = " " },
		func(c *jwt.VerifierConfig) { c.Audience = "" },
		func(c *jwt.VerifierConfig) { c.KeyFunc = nil },
		func(c *jwt.VerifierConfig) { c.Leeway = -time.Second },
	} {
		cfg := config("HS256", key)
		change(&cfg)
		if _, err := jwt.NewVerifier(cfg, func() *claims { return &claims{} }); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
	if _, err := jwt.NewVerifier[*claims](config("HS256", key), nil); err == nil {
		t.Fatal("accepted nil factory")
	}
	v, err := jwt.NewVerifier(config("HS256", key), func() *claims { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(t.Context(), "ignored"); err == nil {
		t.Fatal("accepted typed-nil claims")
	}
	valueVerifier, err := jwt.NewVerifier(config("HS256", key), func() sdkjwt.RegisteredClaims { return sdkjwt.RegisteredClaims{} })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := valueVerifier.Verify(t.Context(), "ignored"); err == nil {
		t.Fatal("accepted value claims that cannot be decoded in place")
	}
	var nilKey *rsa.PrivateKey
	for _, cfg := range []jwt.SignerConfig{
		{Algorithm: "none", Key: key}, {Algorithm: "", Key: key},
		{Algorithm: "RS256"}, {Algorithm: "RS256", Key: nilKey},
		{Algorithm: "HS256", Key: []byte("short")}, {Algorithm: "HS256", Key: "not bytes"},
		{Algorithm: "HS384", Key: key[:47]}, {Algorithm: "HS512", Key: key[:63]},
	} {
		if _, err := jwt.NewSigner(cfg); err == nil {
			t.Fatalf("accepted invalid signer config with algorithm %s", cfg.Algorithm)
		}
	}
	s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: "RS256", Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sign(t.Context(), validClaims()); !errors.Is(err, sdkjwt.ErrInvalidKeyType) {
		t.Fatalf("SDK error identity lost: %v", err)
	}
	if _, err := s.Sign(t.Context(), (*claims)(nil)); err == nil {
		t.Fatal("accepted typed-nil signing claims")
	}
}

func TestLeewayAndMapClaims(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	c := validClaims()
	c.ExpiresAt = sdkjwt.NewNumericDate(time.Now().Add(-time.Minute))
	cfg := config("HS256", key)
	cfg.Leeway = 2 * time.Minute
	if _, err := verifier(t, cfg).Verify(t.Context(), sign(t, "HS256", key, c)); err != nil {
		t.Fatalf("leeway: %v", err)
	}
	v, err := jwt.NewVerifier(cfg, func() sdkjwt.MapClaims { return sdkjwt.MapClaims{} })
	if err != nil {
		t.Fatal(err)
	}
	mapClaims := sdkjwt.MapClaims{
		"iss": "issuer", "aud": "api", "sub": "user-1",
		"exp": time.Now().Add(time.Hour).Unix(), "id": int64(9007199254740993),
	}
	got, err := v.Verify(t.Context(), sign(t, "HS256", key, mapClaims))
	if err != nil || got["id"] != json.Number("9007199254740993") {
		t.Fatalf("map claims precision: %v, %v", got, err)
	}
	mapClaims["exp"] = "tomorrow"
	if got, err := v.Verify(t.Context(), sign(t, "HS256", key, mapClaims)); got != nil || err == nil {
		t.Fatalf("accepted malformed exp: %v, %v", got, err)
	}
}

func TestKeyOwnershipAndErrors(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: "HS256", Key: key})
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	raw, err := s.Sign(t.Context(), validClaims())
	if err != nil {
		t.Fatal(err)
	}
	original := []byte(strings.Repeat("s", 32))
	if _, err := verifier(t, config("HS256", original)).Verify(t.Context(), raw); err != nil {
		t.Fatalf("signer retained caller bytes: %v", err)
	}
	errKey := errors.New("key unavailable")
	cfg := config("HS256", nil)
	cfg.KeyFunc = func(context.Context, *sdkjwt.Token) (any, error) { return nil, errKey }
	if got, err := verifier(t, cfg).Verify(t.Context(), raw); got != nil || !errors.Is(err, errKey) {
		t.Fatalf("key error identity: %v, %v", got, err)
	}
	for _, key := range []any{nil, []byte("short"), "wrong type"} {
		if got, err := verifier(t, config("HS256", key)).Verify(t.Context(), raw); got != nil || err == nil {
			t.Fatalf("accepted invalid verification key: %v, %v", got, err)
		}
	}
}

func TestKeyRotation(t *testing.T) {
	public1, private1, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	public2, private2, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"old": public1, "new": public2}
	errUnknownKey := errors.New("unknown key ID")
	cfg := config("EdDSA", nil)
	cfg.KeyFunc = func(_ context.Context, token *sdkjwt.Token) (any, error) {
		kid, _ := token.Header["kid"].(string)
		key, ok := keys[kid]
		if !ok {
			return nil, errUnknownKey
		}
		return key, nil
	}
	v := verifier(t, cfg)
	for _, tc := range []struct {
		kid string
		key ed25519.PrivateKey
	}{
		{"old", private1}, {"new", private2}, {"unknown", private2},
		{"new", private1},
	} {
		s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: "EdDSA", Key: tc.key, KeyID: tc.kid})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := s.Sign(t.Context(), validClaims())
		if err != nil {
			t.Fatal(err)
		}
		_, err = v.Verify(t.Context(), raw)
		switch {
		case tc.kid == "unknown":
			if !errors.Is(err, errUnknownKey) {
				t.Fatalf("unknown kid: %v", err)
			}
		case tc.kid == "new" && tc.key.Equal(private1):
			if !errors.Is(err, sdkjwt.ErrTokenSignatureInvalid) {
				t.Fatalf("key ID substitution: %v", err)
			}
		default:
			if err != nil {
				t.Fatalf("overlapping rotation keys: %v", err)
			}
		}
	}
}

func TestEd25519KeyOwnership(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: "EdDSA", Key: private})
	if err != nil {
		t.Fatal(err)
	}
	clear(private)
	raw, err := s.Sign(t.Context(), validClaims())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier(t, config("EdDSA", public)).Verify(t.Context(), raw); err != nil {
		t.Fatalf("signer retained caller Ed25519 bytes: %v", err)
	}
}

func TestCancellation(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	errCanceled := errors.New("request stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errCanceled)
	v := verifier(t, config("HS256", key))
	if _, err := v.Verify(ctx, "ignored"); !errors.Is(err, errCanceled) {
		t.Fatalf("verify cancellation: %v", err)
	}
	s, err := jwt.NewSigner(jwt.SignerConfig{Algorithm: "HS256", Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sign(ctx, validClaims()); !errors.Is(err, errCanceled) {
		t.Fatalf("sign cancellation: %v", err)
	}
	started := make(chan struct{})
	cfg := config("HS256", key)
	cfg.KeyFunc = func(ctx context.Context, _ *sdkjwt.Token) (any, error) {
		close(started)
		<-ctx.Done()
		return key, nil
	}
	v = verifier(t, cfg)
	ctx, cancel = context.WithCancelCause(t.Context())
	defer cancel(nil)
	done := make(chan error, 1)
	raw := sign(t, "HS256", key, validClaims())
	go func() {
		_, err := v.Verify(ctx, raw)
		done <- err
	}()
	<-started
	cancel(errCanceled)
	if err := <-done; !errors.Is(err, errCanceled) {
		t.Fatalf("cancellation during key lookup: %v", err)
	}
}

func TestConcurrentRequestClaims(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	v := verifier(t, config("HS256", key))
	var wg sync.WaitGroup
	for i := range 20 {
		c := validClaims()
		c.Subject = fmt.Sprintf("user-%d", i)
		raw := sign(t, "HS256", key, c)
		wg.Go(func() {
			got, err := v.Verify(t.Context(), raw)
			if err != nil || got.Subject != c.Subject {
				t.Errorf("request claims leaked: %v, %v", got, err)
				return
			}
			got.Role = "changed by handler"
		})
	}
	wg.Wait()
}

func TestAuthMiddlewareComposition(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	v := verifier(t, config("HS256", key))
	calls := 0
	handler := auth.New(v.Verify)(endpoint.Endpoint(func(ctx context.Context, _ any) (any, error) {
		calls++
		p, ok := auth.FromContext[*claims](ctx)
		if !ok || p.Subject != "user-1" || p.Role != "reader" {
			t.Fatalf("missing principal: %v", p)
		}
		return p.Subject, nil
	}))
	for _, raw := range []string{sign(t, "HS256", key, validClaims()), "invalid", ""} {
		ctx := metadata.NewContext(t.Context(), metadata.Info{Header: metadata.Pairs("authorization", "Bearer "+raw)})
		got, err := handler(ctx, nil)
		if raw == "invalid" || raw == "" {
			authErr, ok := errors.AsType[*errorx.Error](err)
			if !ok || authErr.Code != errorx.Unauthenticated {
				t.Fatalf("invalid token status: %v", err)
			}
		} else if err != nil || got != "user-1" {
			t.Fatalf("authenticated endpoint: %v, %v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d; want 1", calls)
	}
	keyFailure := errorx.New(errorx.Unavailable, "KEY_UNAVAILABLE", "key lookup unavailable")
	cfg := config("HS256", nil)
	cfg.KeyFunc = func(context.Context, *sdkjwt.Token) (any, error) { return nil, keyFailure }
	failing := auth.New(verifier(t, cfg).Verify)(func(context.Context, any) (any, error) {
		t.Fatal("handler called after key lookup failure")
		return nil, nil
	})
	ctx := metadata.NewContext(t.Context(), metadata.Info{Header: metadata.Pairs("authorization", "Bearer "+sign(t, "HS256", key, validClaims()))})
	if _, err := failing(ctx, nil); !errors.Is(err, keyFailure) || errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("key lookup status was not preserved: %v", err)
	}
}
