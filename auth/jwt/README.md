# auth/jwt

`github.com/nostosx/servex-contrib/auth/jwt` adapts
[`github.com/golang-jwt/jwt/v5`](https://github.com/golang-jwt/jwt) v5.3.1 for
application-owned JWT signing and typed verification. Cryptography, JSON claims
decoding and standard claim validation are delegated to the SDK. Production
code imports only the SDK and standard library; middleware composition tests
also require published servex and middleware v1.4.0.

This module is released as v1.0.0 and requires Go 1.27.3. Install the independent v1.0.0 module. Its license and the SDK's license are MIT.

## API

| API | Behavior |
|---|---|
| `NewSigner(SignerConfig)` | Selects an explicit algorithm, signing key and optional key ID |
| `signer.Sign(ctx, claims)` | Signs application-supplied SDK claims without changing them |
| `NewVerifier(VerifierConfig, newClaims)` | Configures one algorithm, trusted issuer, expected audience and context-aware key callback |
| `verifier.Verify(ctx, raw)` | Returns typed claims only after signature and claims verification; directly usable with `auth.New` |

Supported algorithms: HS256/384/512, RS256/384/512, PS256/384/512,
ES256/384/512 and EdDSA. Each verifier accepts exactly one algorithm; there is
no implicit default, algorithm negotiation or `none` support. Issuer, audience,
expiration and a non-empty subject are mandatory. `nbf` and `iat` are checked
when present. `Leeway` defaults to zero and must be non-negative. No parser
option can disable these checks. Custom claims may implement the SDK's
`ClaimsValidator` for additional application checks.

Keys use native SDK types: `[]byte` for HMAC, `*rsa.PrivateKey`/`*rsa.PublicKey`
for RSA and PSS, `*ecdsa.PrivateKey`/`*ecdsa.PublicKey` for ECDSA, and
`ed25519.PrivateKey`/`ed25519.PublicKey` for EdDSA. HMAC secrets must contain at
least 32/48/64 bytes for HS256/384/512 respectively and should be generated
with cryptographic randomness. The signer copies byte-slice keys. Other keys
remain application-owned and must not be mutated concurrently. The SDK reports
incompatible signing key types from `Sign`.

## Usage

This complete program generates a disposable Ed25519 key, signs one token and
passes it through the existing transport-independent authentication middleware.
Production applications load persistent keys from their own trusted storage.

```go
package main

import (
    "context"
    "crypto/ed25519"
    "crypto/rand"
    "errors"
    "fmt"
    "log"
    "time"

    sdkjwt "github.com/golang-jwt/jwt/v5"

    "github.com/nostosx/servex/metadata"
    "github.com/nostosx/servex-contrib/auth/jwt"
    "github.com/nostosx/servex-contrib/middleware/auth"
)

type Claims struct {
    sdkjwt.RegisteredClaims
    Role string `json:"role"`
}

func main() {
    ctx := context.Background()
    public, private, err := ed25519.GenerateKey(rand.Reader)
    if err != nil {
        log.Fatal(err)
    }
    signer, err := jwt.NewSigner(jwt.SignerConfig{
        Algorithm: "EdDSA", Key: private, KeyID: "key-1",
    })
    if err != nil {
        log.Fatal(err)
    }
    verifier, err := jwt.NewVerifier(jwt.VerifierConfig{
        Algorithm: "EdDSA", Issuer: "example-login", Audience: "example-api",
        KeyFunc: func(_ context.Context, token *sdkjwt.Token) (any, error) {
            if token.Header["kid"] != "key-1" {
                return nil, errors.New("unknown key")
            }
            return public, nil
        },
    }, func() *Claims { return &Claims{} })
    if err != nil {
        log.Fatal(err)
    }
    raw, err := signer.Sign(ctx, &Claims{
        RegisteredClaims: sdkjwt.RegisteredClaims{
            Issuer: "example-login", Audience: sdkjwt.ClaimStrings{"example-api"},
            Subject: "user-1", ExpiresAt: sdkjwt.NewNumericDate(time.Now().Add(15*time.Minute)),
        },
        Role: "reader",
    })
    if err != nil {
        log.Fatal(err)
    }
    handler := auth.New(verifier.Verify)(func(ctx context.Context, _ any) (any, error) {
        principal, _ := auth.FromContext[*Claims](ctx)
        return principal.Subject + " " + principal.Role, nil
    })
    // A servex transport populates this metadata from the incoming request.
    ctx = metadata.NewContext(ctx, metadata.Info{
        Kind: metadata.KindServer, Header: metadata.Pairs("authorization", "Bearer "+raw),
    })
    result, err := handler(ctx, nil)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result) // user-1 reader
}
```

In an HTTP application, compose `auth.New(verifier.Verify)` with
`httpx.WithMiddleware`. For gRPC streams, opt in through
`grpcx.WithStreamMiddleware`. The handler reads the exact factory result type
with `auth.FromContext[*Claims]`. Ordinary verification errors become
`Unauthenticated`; application-provided `errorx` errors retain their status.

## Ownership and validation

Reuse signers and verifiers across requests. The claims factory must return a
fresh non-nil pointer or initialized map for each verification, and embedded
pointer claims must be initialized before use. Prefer embedding
`sdkjwt.RegisteredClaims` by value. Claims factories and key callbacks must be
safe for concurrent use. Map claims preserve numbers as `json.Number`.

`KeyFunc` receives an **unverified** token. Use `kid` only to select from
trusted application-configured keys; reject unknown IDs. Never fetch a URL
or select a trust boundary from unverified headers or claims. The callback
returns one key in the configured algorithm's native verification type. The
application owns key loading, rotation and any bounded/cached remote lookup;
this module does not fetch JWKS. Existing `auth/oidc` remains the adapter for
OIDC ID Tokens and provider discovery.

Signing does not populate or validate application claims. The application
sets issuer, audience, subject and a bounded expiration before signing.
Verification enforces the configured policy. Authentication does not establish
authorization: permissions, login sessions, refresh-token storage, revocation
and token-purpose separation belong to the application.

Cancellation is checked before and after synchronous signing/verification.
Key callbacks receive the request context and must honor cancellation and
timeouts for I/O; cryptographic work already running cannot be interrupted.
Errors wrap their SDK/callback causes for `errors.Is/As`; the adapter never
logs tokens or key material.

## Checks

From the repository root:

```sh
task check lint vulncheck MODULES='auth/jwt'
```

Tests generate local keys and need no services. They exercise all supported
algorithms, tampering, algorithm substitution, claims policy, custom claims,
key errors, cancellation, concurrent claim isolation and auth composition.
The runnable example is also checked by `go test`.
