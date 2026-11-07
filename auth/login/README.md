# auth/login

Module version for this revision: **v1.3.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

`login` combines OAuth2, OIDC and the owned Session implementation around an
application-defined principal. Browser and JWT handlers can both read that type
through `middleware/endpoint.PrincipalFromContext[P]`. Password verification stays in the
application; after verification call `SignIn`.

Configure one Session Manager and one reusable login Manager in `app.go`:

```go
manager, err := login.New(sessions, login.Config[Principal]{
    Valid: func(p Principal) bool { return p.ID != "" },
    SuccessURL: "/workspace",
    OIDC: &login.OIDC[Principal]{
        Client: oauthClient,
        Verifier: idTokenVerifier,
        Resolve: resolveVerifiedAccount,
    },
})
```

This is a lower-level OIDC assembly fragment. Its `Resolve` receives a verified
ID token and maps issuer/subject to an application account with its own policy.
The common provider configuration below instead resolves `client.Identity`.
Omit both `OIDC` and `Provider` for password-only applications.
This module does not store access/refresh tokens in the session.

Mount `manager.Middleware` once around the router. Register `GET /auth/login`
with `Begin`, the adapter's callback method/path with `Callback`, and
`POST /auth/logout` with `Logout`. OIDC query callbacks use GET; Apple uses POST.
Protect browser handlers with `Require`. Password handlers call
`SignIn(r.Context(), principal)` after checking credentials, before writing their
response. Session persistence completes before the response or redirect commits;
store errors suppress a successful response.

The flow binds one-time OAuth state to the managed browser Session and rotates
the session token on authentication. Provider adapters send PKCE, verify OIDC
nonce/signatures or resolve ordinary OAuth2 identities through authenticated APIs.
It creates no additional binding cookie. State is consumed before token exchange;
retries after a failed or uncertain callback require a new login. Logout deletes
the session, so another replica immediately rejects the same cookie.

For replicas, share an official Redis client between
[`session/redis`](../../session/redis) and [`auth/oauth2/redis`](../oauth2/redis),
with separate key prefixes. Configure finite timeouts and `MaxRetries: -1` for
non-repeatable `GETDEL`. Memory stores are process-local. Retain secure cookies
in production. Mount CORS and standard-library `http.CrossOriginProtection`
outside Session/login middleware with explicit trusted origins. The application
still owns authorization and account mapping.

Validate this module and its related adapters from the contrib root:

```sh
task check lint vulncheck \
  MODULES='auth/login auth/oauth2/redis'
```

Tests use a locally signed OIDC/JWKS fixture and memory/miniredis backends;
they do not establish external provider acceptance.

## Common third-party providers

Construct client.NewProvider for Google, GitHub, Microsoft, GitLab, Discord,
LinkedIn, Apple or a trusted OIDC issuer. Select Config.Provider with
Provider[P]{Client, Resolve} to map its verified client.Identity to your principal.
Select either Provider or the lower-level OIDC configuration, never both.
Keep a distinct SessionKey/manager and exact callback route for each provider.
Resolve owns explicit account linkage and access policy; matching email must
not automatically link accounts.

Begin is GET. Callback uses the adapter's CallbackMethod (GET normally, POST
for Apple). Apple requires Secure SameSite=None session cookies and an exact
callback-only exemption from generic cross-origin POST/CSRF rejection.
Callback still validates original session binding, one-time state and ID-token
signature/nonce. Its form body is limited to 16 KiB and requires
application/x-www-form-urlencoded. It never falls back to query fields.
Duplicate state/code and provider errors fail closed. Session rotation and
persistence precede success. Other mutation routes retain CSRF protection.

See [provider setup and limits](../oauth2/README.md). Fixtures do not establish
live application approval or third-party account/browser login.

## Enterprise SAML SSO

Use `Config.SAML` with `SAML[P]{Client, Resolve}` and a configured
[`auth/saml.ServiceProvider`](../saml). Select exactly one of OIDC, Provider or
SAML in a manager. Register the adapter's SP metadata separately as a public GET
route, Begin as GET and this manager's Callback on the exact POST ACS route.
SAML's signed assertion, correlation state and original browser Session must all
validate before Resolve and local Session rotation.

The ACS requires Secure HttpOnly SameSite=None sessions and an exact-path
cross-origin POST/CSRF exception. It accepts a bounded 2 MiB form body with one
RelayState and one SAMLResponse. It does not take those fields from query values,
accept duplicate fields or process SAMLart. Complete caps encoded assertions at
1 MiB. Unsolicited IdP login and SLO are not enabled. All other mutation routes
retain normal CSRF protection.

Resolve receives issuer/NameID, format/qualifiers, attributes, SessionIndex and
expiry. Scope account linkage to the configured tenant/IdP and preserve NameID
qualifiers. Do not auto-link by email or map IdP groups to privileged local roles
without explicit policy. Include expiry in the application principal/Valid
policy or otherwise restrict the local session lifetime. The module does not
automatically revoke local sessions when an enterprise IdP session ends.

`Config` gains SAML and OAuth2 Identity gains UnionID in the corresponding minor
releases. Use keyed struct literals and upgrade imports/requirements together.
