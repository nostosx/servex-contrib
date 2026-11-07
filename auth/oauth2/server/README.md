# auth/oauth2/server

Module version for this revision: **v1.2.0**.

OAuth2 authorization-server and OIDC provider adapter using Ory Fosite v0.49.0.
Use Go 1.27.3. This package shares the auth/oauth2 module with the client package;
applications import either package explicitly. The server imports no application
account model. Fosite implements protocol validation, PKCE,
token generation, ID-token signing and protocol responses.

## Responsibilities

`New(Config, Store)` selects authorization code + mandatory S256 PKCE,
refresh-token rotation/reuse detection, client credentials, revocation,
introspection and code-flow OIDC. Implicit, password, hybrid, assertion and
device grants are deliberately not enabled. Scopes and audiences use exact
matching. The native `fosite.Config` supplies finite lifetimes, the global HMAC
secret and a client-secret hasher. Error debug details are never returned.

The application owns authentication, consent, allowed scopes/audiences,
client lifecycle and the user session. It calls `Server.Provider` to validate
an authorization request, grants approved scopes/audiences, supplies an
`openid.DefaultSession`, and writes the native authorization response. It
implements UserInfo by introspecting the access token and selecting approved
claims. `OnToken` can append application-owned issuance audit records before
the token response is written. It must never record raw tokens or secrets.

`Token`, `Revoke`, `Introspect`, `Discovery` and `JWKS` are HTTP handlers. Mount
them at the paths declared in Discovery. Apply body limits, timeouts, transport
security, trusted-origin policy and rate limits in the service's composition
root. Discovery declares the supported methods; individual clients can be more
restricted. Machine grants never receive ID/refresh tokens or user-only scopes.
Introspection requires authenticated HTTP Basic credentials and reveals an
active token only to its issuing client or a named audience client.

## Keys and credentials

Configure a persistent RSA private key of at least 2048 bits and a nonempty
key ID. `LoadRSAKey` accepts PKCS#1/PKCS#8 PEM from trusted local paths.
Only public JWKs are returned by JWKS; retain old public keys through
`Config.PublicKeys` until previously issued ID tokens expire. The issuer must
use HTTPS, except explicit loopback-only development opt-in. Do not derive the
issuer or key lookup paths from request headers or unverified tokens.

The global HMAC secret must have at least 32 bytes and be identical across
replicas. Replacing it invalidates outstanding opaque credentials unless
rotated secrets are supplied through the native SDK configuration. Signing-key
rotation and HMAC-secret rotation are separate operations.

`NewPasswordHasher(passwords, parallelism)` accepts the native Hash/Verify
methods of the contrib password adapter. It bounds concurrent KDF operations
and rejects oversized client credentials. Applications persist hashes and
return generated client secrets only once.

## PostgreSQL persistence

`PostgresStore{DB, Clients, Check}` implements the SDK's storage and transaction
interfaces. `Clients` loads live application policy; `Check` revalidates current
account revisions, consent or other business revocation rules on every stored
credential read. The store uses `openid.DefaultSession`, not arbitrary session
implementations.

Call `ApplyPostgresSchema(ctx, db)` explicitly from a migration command. It
serializes concurrent schema initialization with a PostgreSQL advisory lock.
Constructors and request handlers never create schema. The fixed
`oauth2_sessions` table belongs to the authorization-server database. A database
driver is selected by the consuming application; pgx is a test dependency here.

Lookup keys are SHA-256 digests, including OIDC's raw authorization-code lookup
key. The store persists sanitized request forms and excludes client secrets.
Session payloads still contain personal information and require appropriate
database access controls. Token families remain available for reuse detection
until expiry. Operators may delete expired rows using the indexed `expires_at`
column. Request-ID indexes support grant-wide revocation; refresh rotation uses
an atomic conditional update inside the SDK's transaction.

## Validation

```sh
task check lint vulncheck MODULES=auth/oauth2
SERVEX_IT_POSTGRES_DSN='postgres://.../owned_disposable_db' \
  task test INTEGRATION=true MODULES=auth/oauth2
```

Unit tests exercise the actual Fosite code/refresh flows, invalid PKCE,
redirect mismatch, reuse detection and public JWKS output. The PostgreSQL
integration requires an explicitly configured disposable database and verifies
cross-instance reads and concurrent one-time refresh consumption. These checks
do not constitute OpenID Foundation conformance certification.
