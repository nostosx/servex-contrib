# auth/oauth2

OAuth2 client and server implementations live in one capability module,
**v1.1.0**, Go 1.27.3, MIT. Dependencies: Go OAuth2 SDK, go-oidc through auth/oidc,
and Ory Fosite.

```text
auth/oauth2/             module github.com/nostosx/servex-contrib/auth/oauth2
  client/               PKCE/state, refresh, third-party identity adapters
  server/               OAuth2/OIDC server and PostgreSQL credential storage
  redis/                independent nested module for atomic shared client state
```

Server and client are packages in the same module. Redis keeps an independent
module/version boundary so other consumers do not require Redis dependencies.
There is no root facade or automatic assembly.

## Login providers

`client.NewProvider` handles code exchange and identity verification. `Begin`
returns a redirect URL; `Complete` returns `Identity`, never browser tokens or
application permissions.

| Kind | Verification | Default scopes | Requirements |
| --- | --- | --- | --- |
| `Google` | Discovery, signed ID token, nonce | openid profile email | Google OAuth web client; normalizes Google's two documented issuer forms |
| `GitHub` | Authenticated /user and /user/emails APIs | read:user user:email | GitHub.com OAuth App; stable numeric ID; verified emails only, preferring primary |
| `Microsoft` | Tenant discovery, signed ID token, nonce | openid profile email | Explicit tenant UUID; common/organizations tenant templates rejected |
| `GitLab` | Discovery, signed ID token, nonce | openid profile email | GitLab.com or trusted self-hosted Issuer |
| `Discord` | Authenticated /users/@me API | identify email | Stable user ID, provider verified-email flag |
| `LinkedIn` | Discovery, signed ID token, nonce | openid profile email | Enable Sign in with LinkedIn using OpenID Connect product |
| `Apple` | Discovery, signed ID token, nonce | openid email | Services ID, current client-secret JWT, POST callback, Secure SameSite=None binding/session |
| `OIDC` | Trusted issuer discovery, signed ID token, nonce | openid profile email | Explicit issuer, e.g. Keycloak/Auth0/Okta/ZITADEL; actual provider acceptance still required |

All adapters send S256 PKCE. Enforcement depends on the provider; Apple and
providers without PKCE enforcement rely on confidential-client authentication,
nonce and state. OIDC checks signature, issuer, audience, expiry, subject,
authorized party and nonce. If at_hash is present it must match the access token.
Email and profile fields may be absent. EmailVerified is a provider assertion
about mailbox verification, not legal identity verification.

Scopes can override defaults. OIDC requires openid. Without GitHub user:email,
no email lookup occurs. An identity API failure fails login. The first 100
GitHub email records are considered; a primary outside that page is not guaranteed
selection. GitHub Enterprise, Microsoft cross-tenant policy, WeChat/QQ/Weibo and
SAML are not built-in adapters.

Official references:
[Google](https://developers.google.com/identity/openid-connect/openid-connect),
[GitHub](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps),
[Microsoft](https://learn.microsoft.com/en-us/entra/identity-platform/v2-protocols-oidc),
[GitLab](https://docs.gitlab.com/integration/openid_connect_provider/),
[Discord](https://docs.discord.com/developers/topics/oauth2),
[LinkedIn](https://learn.microsoft.com/en-us/linkedin/consumer/integrations/self-serve/sign-in-with-linkedin-v2),
[Apple](https://developer.apple.com/documentation/signinwithapplerestapi).

## Composition

Import `github.com/nostosx/servex-contrib/auth/oauth2/client`:

```go
states, err := client.NewMemoryStore(10000) // Single process only.
if err != nil { return err }
provider, err := client.NewProvider(ctx, client.ProviderConfig{
    Kind: client.Google,
    ClientID: clientID, ClientSecret: clientSecret,
    RedirectURL: "https://accounts.example.com/login/google/callback",
}, states)
if err != nil { return err }
authorization, err := provider.Begin(ctx, browserBinding)
if err != nil { return err }
// Persist the random browser/session binding before redirecting to authorization.URL.
// On callback, restore that binding and read state/code from the query or Apple form.
identity, err := provider.Complete(ctx, browserBinding, state, code)
if err != nil { return err }
// Resolve (identity.Issuer, identity.Subject) in the application account store.
// Rotate/persist the application session only after its account policy succeeds.
```

[`auth/login`](../login) composes Provider with browser sessions and an application
Resolve callback. Give each provider its own manager, SessionKey and exact callback
route. Account creation/linking/unlinking, tenant policy and audit belong to the
account service. Never auto-link accounts based on matching email.

Apple uses application/x-www-form-urlencoded POST. Select Secure SameSite=None
session cookies and exclude only the exact callback from generic cross-origin
POST/CSRF rejection. State, nonce and original session binding remain mandatory.
Do not trust callback user/name fields as signed identity. Generate the expiring
client-secret JWT with a vetted JWT signer, keep the private key outside config,
and rebuild/rotate the provider before secret expiry. This adapter does not issue
Apple developer credentials.

Provider settings are trusted administrator input. HTTPS is required;
AllowInsecureLoopback allows HTTP loopback fixtures only. Backend requests default
to ten-second timeouts, reject redirects and limit bodies to 1 MiB. HTTPClient is
copied; its Transport is borrowed. Keep the construction context alive for the
verifier's key refresh. Do not log SDK error bodies, secrets, state or tokens.
Return generic login errors to browsers.

Multiple providers can share StateStore. Attempts bind provider/issuer, client ID,
redirect URI, protocol endpoints and browser/session. State is consumed before
exchange; failed/uncertain exchanges need a fresh login. TTL defaults to five
minutes (one second to ten minutes). MemoryStore is bounded and loses pending
logins on restart; replicas need [redis](redis) or another atomic shared store.

## Protocol client and server

`client.New` permits explicit OAuth endpoints. Its Result is not an authenticated
identity: validate an ID token or call the provider identity API before login.
TokenSource/ClientCredentials use official SDK behavior. Select a finite-timeout
HTTP client through oauth2.HTTPClient for these lower-level calls and explicitly
persist refreshed tokens.

[server](server) provides authorization-code/PKCE, client credentials, refresh
rotation/reuse detection, revocation/introspection, discovery/JWKS, signed ID
tokens, UserInfo and explicit PostgreSQL migrations. Application clients, consent
and account/session revocation policies remain application-owned.

## Migration and validation

| Old import/module | New import/module |
| --- | --- |
| auth/oauth2 client root package | auth/oauth2/client |
| auth/oauth2server | auth/oauth2/server (parent module auth/oauth2) |
| auth/oauth2redis | auth/oauth2/redis (nested module) |

Package names are client, server and redis. Upgrade qualifiers and requirements
together. Old tags remain immutable; no forwarding packages are published.
Pending old client states lack client binding and fail closed: begin a new login.
Server SQL tables, credential format and schema advisory-lock identity are retained.

```sh
task check lint vulncheck MODULES='auth/oauth2 auth/oauth2/redis auth/login auth/oidc'
SERVEX_IT_POSTGRES_DSN='...' task test INTEGRATION=true MODULES=auth/oauth2
SERVEX_IT_REDIS_ADDR='...' task test INTEGRATION=true MODULES=auth/oauth2/redis
```

Protocol fixtures verify exchange, PKCE, user APIs, signed token claims, replay,
provider/client mixup, browser binding, bounded reads and redirect rejection.
They do not prove live Google/GitHub/Apple login, app approval or console settings.
