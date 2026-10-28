# auth/oidc

`github.com/nostosx/servex-contrib/auth/oidc` verifies OpenID Connect ID tokens
using [coreos/go-oidc](https://github.com/coreos/go-oidc). It provides a typed
verification callback; it does not install HTTP routes, manage login sessions,
or import another contrib module.

This module is unreleased and requires Go 1.27.3. It adds one direct SDK
dependency, `github.com/coreos/go-oidc/v3 v3.21.0` (Apache-2.0). This module's
own license is MIT.

## API

| API | Behavior |
|---|---|
| `New(ctx, issuer, audience, opts...)` | Discovers the provider, validates its issuer and JWKS URL, and builds a reusable verifier |
| `NewRemote(ctx, issuer, audience, jwksURL, opts...)` | Uses a trusted explicit JWKS endpoint; the first key fetch is lazy |
| `Verify(ctx, raw)` | Verifies an ID token and returns `*IDToken`; custom claims are available through its `Claims` method |
| `VerifyNonce(ctx, raw, expected)` | Additionally checks the non-empty nonce saved for this login attempt |
| `WithHTTPClient(client)` | Selects TLS/proxy/transport configuration; the client is copied and a missing timeout defaults to 10s |
| `WithSigningAlgorithms(...)` | Replaces the default RS256 allowlist with explicitly permitted asymmetric algorithms |
| `WithInsecureHTTP()` | Permits HTTP only for trusted local test providers |

All verification checks are mandatory: signature, exact issuer, expected
audience, expiration and non-empty subject. An `azp` claim must equal the
configured audience when present; tokens with several audiences must include
it. The SDK checks `nbf` with five minutes of clock leeway. Discovery and JWKS
URLs require HTTPS without userinfo or fragments, and redirects cannot
downgrade to HTTP. The issuer cannot contain a query. Algorithms `none` and
HMAC are rejected. The adapter exposes no skip-validation options.

Reuse the verifier across requests. The SDK caches signing keys and fetches
them again when it encounters an unknown key ID. Each `Verify` responds to its
caller's cancellation. Shared JWKS fetches use the configured HTTP timeout and
may continue for other waiters after one caller cancels.

## Usage

This complete program validates the ID token supplied by an application-owned
login callback. Set `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_ID_TOKEN` and the nonce
saved in that login session as `OIDC_EXPECTED_NONCE` before running it:

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/nostosx/servex-contrib/auth/oidc"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    verifier, err := oidc.New(ctx, os.Getenv("OIDC_ISSUER"), os.Getenv("OIDC_CLIENT_ID"))
    if err != nil {
        log.Fatal(err)
    }
    principal, err := verifier.VerifyNonce(ctx, os.Getenv("OIDC_ID_TOKEN"), os.Getenv("OIDC_EXPECTED_NONCE"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("authenticated subject: %s", principal.Subject)
}
```

The application owns OAuth state validation, nonce generation, session binding
and one-time nonce consumption. `Verify` alone does not provide those login
protections. These APIs verify ID tokens; they do not introspect opaque OAuth
access tokens, and accepting an ID token does not authorize API access.

For endpoints whose protocol deliberately accepts ID tokens, compose
`auth.New(verifier.Verify)` from `servex-contrib/middleware/auth` in the
application, or inject a callback using `VerifyNonce` when the route belongs to
a login flow. Use `auth.FromContext[*oidc.IDToken]` to access the principal.
This keeps the SDK adapter independent of the middleware module. Invalid tokens
return errors without logging their contents; the authentication middleware
maps ordinary verification errors to `Unauthenticated`. Authorization policies
remain application-owned.

For example, the application's composition root can use this assembly fragment
after successfully constructing `verifier`:

```go
import (
    "github.com/nostosx/servex-contrib/middleware/auth"
    "github.com/nostosx/servex/transport/httpx"
)

srv := httpx.NewServer(httpx.WithMiddleware(auth.New(verifier.Verify)))
// Register the application routes that deliberately accept these ID tokens.
```

When the same verifier protects a gRPC streaming RPC, opt in explicitly through
`grpcx.WithStreamMiddleware(auth.New(verifier.Verify))`.

## Tests

All tests use dedicated `httptest` providers and locally generated signing
keys. They cover discovery and explicit JWKS, key caching and rotation,
signature tampering, issuer/audience/expiry/subject/azp/nonce rejection,
algorithm restrictions, cancellation and HTTPS downgrade rejection. No hosted
identity provider, real credentials or external service is required.

From the repository root:

```sh
MODULES='auth/oidc' task check lint vulncheck
```
