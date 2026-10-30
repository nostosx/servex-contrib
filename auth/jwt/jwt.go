// Package jwt adapts golang-jwt/jwt for application-owned token signing and
// typed verification. Verifier.Verify can be passed directly to auth.New.
package jwt

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	sdkjwt "github.com/golang-jwt/jwt/v5"
)

// SignerConfig selects one explicit algorithm and its signing key. KeyID, when
// non-empty, is written to the kid header. Keys use the SDK's native Go types;
// PEM decoding and key loading belong to the application.
type SignerConfig struct {
	Algorithm string
	Key       any
	KeyID     string
}

// Signer signs supplied claims without adding or changing them. Reuse it across
// requests; callers must not mutate asymmetric keys or claims during signing.
// HMAC key bytes are copied at construction.
type Signer struct {
	method sdkjwt.SigningMethod
	key    any
	keyID  string
}

// NewSigner constructs a signer. It rejects unsupported algorithms and missing
// keys. Key type/format errors are returned by the SDK from Sign.
func NewSigner(config SignerConfig) (*Signer, error) {
	method, err := signingMethod(config.Algorithm)
	if err != nil {
		return nil, err
	}
	if isNil(config.Key) {
		return nil, errors.New("jwt: signing key is required")
	}
	key := config.Key
	switch secret := key.(type) {
	case []byte:
		key = slices.Clone(secret)
	case ed25519.PrivateKey:
		key = slices.Clone(secret)
	}
	if err := checkHMACKey(method, key); err != nil {
		return nil, err
	}
	return &Signer{method: method, key: key, keyID: config.KeyID}, nil
}

// Sign signs claims with the configured algorithm and key. The application owns
// issuer, audience, subject, expiry and any custom claims. Cancellation is
// checked before and after the synchronous SDK operation; it cannot interrupt
// cryptographic work already in progress. Errors preserve their SDK cause.
func (s *Signer) Sign(ctx context.Context, claims sdkjwt.Claims) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	if isNil(claims) {
		return "", errors.New("jwt: claims are required")
	}
	token := sdkjwt.NewWithClaims(s.method, claims)
	if s.keyID != "" {
		token.Header["kid"] = s.keyID
	}
	raw, err := token.SignedString(s.key)
	if cause := context.Cause(ctx); cause != nil {
		return "", cause
	}
	if err != nil {
		return "", fmt.Errorf("jwt: sign token: %w", err)
	}
	return raw, nil
}

// KeyFunc resolves one trusted verification key. The token has NOT been verified
// yet: only use its kid to select from application-configured keys, never to
// choose an arbitrary URL or trust its claims. Honor ctx when fetching keys.
// The callback and returned asymmetric keys must be safe for concurrent use.
type KeyFunc func(ctx context.Context, token *sdkjwt.Token) (any, error)

// VerifierConfig selects the token's trust boundary. Algorithm, Issuer, Audience
// and KeyFunc are mandatory. Exactly one algorithm is accepted, preventing
// accidental HMAC/asymmetric algorithm mixing. Leeway defaults to zero and must
// be non-negative. No option disables signature or standard claim validation.
type VerifierConfig struct {
	Algorithm string
	Issuer    string
	Audience  string
	KeyFunc   KeyFunc
	Leeway    time.Duration
}

// Verifier verifies tokens into request-owned claims. It is safe for concurrent
// use when KeyFunc and newClaims are; newClaims must return a fresh non-nil
// pointer or map each time, with any embedded pointer claims initialized.
type Verifier[P sdkjwt.Claims] struct {
	parser    *sdkjwt.Parser
	keyFunc   KeyFunc
	newClaims func() P
}

// NewVerifier builds a typed verifier. Signature, issuer, audience, expiration
// and non-empty subject are required. nbf and iat are checked when present.
// Custom claims can implement the SDK's ClaimsValidator for additional checks.
func NewVerifier[P sdkjwt.Claims](config VerifierConfig, newClaims func() P) (*Verifier[P], error) {
	if _, err := signingMethod(config.Algorithm); err != nil {
		return nil, err
	}
	if strings.TrimSpace(config.Issuer) == "" || strings.TrimSpace(config.Audience) == "" {
		return nil, errors.New("jwt: issuer and audience are required")
	}
	if config.KeyFunc == nil || newClaims == nil {
		return nil, errors.New("jwt: key function and claims factory are required")
	}
	if config.Leeway < 0 {
		return nil, errors.New("jwt: leeway must be non-negative")
	}
	parser := sdkjwt.NewParser(
		sdkjwt.WithValidMethods([]string{config.Algorithm}),
		sdkjwt.WithIssuer(config.Issuer),
		sdkjwt.WithAudience(config.Audience),
		sdkjwt.WithExpirationRequired(),
		sdkjwt.WithIssuedAt(),
		sdkjwt.WithLeeway(config.Leeway),
		sdkjwt.WithJSONNumber(),
		sdkjwt.WithStrictDecoding(),
	)
	return &Verifier[P]{parser: parser, keyFunc: config.KeyFunc, newClaims: newClaims}, nil
}

// Verify returns claims only after successful verification. SDK errors and
// callback errors remain discoverable through errors.Is/As. Cancellation takes
// precedence, including after a key lookup or synchronous verification.
func (v *Verifier[P]) Verify(ctx context.Context, raw string) (P, error) {
	var zero P
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	claims := v.newClaims()
	if isNil(claims) {
		return zero, errors.New("jwt: claims factory returned nil")
	}
	if kind := reflect.TypeOf(claims).Kind(); kind != reflect.Pointer && kind != reflect.Map {
		return zero, errors.New("jwt: claims factory must return a pointer or map")
	}
	_, err := v.parser.ParseWithClaims(raw, claims, func(token *sdkjwt.Token) (any, error) {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		key, err := v.keyFunc(ctx, token)
		if err != nil {
			return nil, err
		}
		if isNil(key) {
			return nil, errors.New("jwt: verification key is required")
		}
		if err := checkHMACKey(token.Method, key); err != nil {
			return nil, err
		}
		return key, nil
	})
	if cause := context.Cause(ctx); cause != nil {
		return zero, cause
	}
	if err != nil {
		return zero, fmt.Errorf("jwt: verify token: %w", err)
	}
	subject, err := claims.GetSubject()
	if err != nil {
		return zero, fmt.Errorf("jwt: read subject: %w", err)
	}
	if strings.TrimSpace(subject) == "" {
		return zero, fmt.Errorf("jwt: subject is required: %w", sdkjwt.ErrTokenRequiredClaimMissing)
	}
	return claims, nil
}

func signingMethod(algorithm string) (sdkjwt.SigningMethod, error) {
	// Resolve only the SDK's built-in methods; never infer policy from a token.
	switch algorithm {
	case "HS256":
		return sdkjwt.SigningMethodHS256, nil
	case "HS384":
		return sdkjwt.SigningMethodHS384, nil
	case "HS512":
		return sdkjwt.SigningMethodHS512, nil
	case "RS256":
		return sdkjwt.SigningMethodRS256, nil
	case "RS384":
		return sdkjwt.SigningMethodRS384, nil
	case "RS512":
		return sdkjwt.SigningMethodRS512, nil
	case "PS256":
		return sdkjwt.SigningMethodPS256, nil
	case "PS384":
		return sdkjwt.SigningMethodPS384, nil
	case "PS512":
		return sdkjwt.SigningMethodPS512, nil
	case "ES256":
		return sdkjwt.SigningMethodES256, nil
	case "ES384":
		return sdkjwt.SigningMethodES384, nil
	case "ES512":
		return sdkjwt.SigningMethodES512, nil
	case "EdDSA":
		return sdkjwt.SigningMethodEdDSA, nil
	default:
		return nil, errors.New("jwt: unsupported signing algorithm")
	}
}

func checkHMACKey(method sdkjwt.SigningMethod, key any) error {
	if hmac, ok := method.(*sdkjwt.SigningMethodHMAC); ok {
		secret, ok := key.([]byte)
		if !ok || len(secret) < hmac.Hash.Size() {
			return fmt.Errorf("jwt: HMAC key must be []byte with at least %d bytes", hmac.Hash.Size())
		}
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
