package jwt_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	sdkjwt "github.com/golang-jwt/jwt/v5"

	"github.com/nostosx/servex-contrib/auth/jwt"
	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex/metadata"
)

type exampleClaims struct {
	sdkjwt.RegisteredClaims
	Role string `json:"role"`
}

func Example() {
	ctx := context.Background()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	signer, err := jwt.NewSigner(jwt.SignerConfig{
		Algorithm: "EdDSA", Key: private, KeyID: "key-1",
	})
	if err != nil {
		panic(err)
	}
	verifier, err := jwt.NewVerifier(jwt.VerifierConfig{
		Algorithm: "EdDSA", Issuer: "example-login", Audience: "example-api",
		KeyFunc: func(_ context.Context, token *sdkjwt.Token) (any, error) {
			if token.Header["kid"] != "key-1" {
				return nil, errors.New("unknown key")
			}
			return public, nil
		},
	}, func() *exampleClaims { return &exampleClaims{} })
	if err != nil {
		panic(err)
	}
	raw, err := signer.Sign(ctx, &exampleClaims{
		Issuer: "example-login", Audience: sdkjwt.ClaimStrings{"example-api"},
		Subject: "user-1", ExpiresAt: sdkjwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		Role: "reader",
	})
	if err != nil {
		panic(err)
	}
	handler := auth.New(verifier.Verify)(func(ctx context.Context, _ any) (any, error) {
		principal, _ := auth.FromContext[*exampleClaims](ctx)
		return principal.Subject + " " + principal.Role, nil
	})
	// A servex transport populates this metadata from the incoming request.
	ctx = metadata.NewContext(ctx, metadata.Info{
		Kind: metadata.KindServer, Header: metadata.Pairs("authorization", "Bearer "+raw),
	})
	result, err := handler(ctx, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
	// Output: user-1 reader
}
