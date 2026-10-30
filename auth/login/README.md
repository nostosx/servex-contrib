# Browser login composition

`login` combines OAuth2, OIDC and the owned Session implementation around an
application-defined principal. Browser and JWT handlers can both read that type
through `middleware/auth.FromContext[P]`. Password verification stays in the
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

This is an assembly fragment. The complete application, including configuration,
password authentication and resource cleanup, lives in
[`servex-examples/services/user`](https://github.com/nostosx/servex-examples/tree/main/services/user).
`Resolve` receives a verified ID token and maps issuer/subject to an application
account with its own access policy. Omit `OIDC` for password-only applications.
This module does not store access/refresh tokens in the session.

Mount `manager.Middleware` once around the router. Register `GET /auth/login`
with `Begin`, `GET /auth/callback` with `Callback`, and `POST /auth/logout` with
`Logout`. Protect browser handlers with `Require`. Password handlers call
`SignIn(r.Context(), principal)` after checking credentials, before writing their
response. Session persistence completes before the response or redirect commits;
store errors suppress a successful response.

The flow binds one-time OAuth state to the managed browser Session, uses PKCE,
verifies the OIDC nonce and rotates the session token on authentication.
It creates no additional binding cookie. State is consumed before token exchange;
retries after a failed or uncertain callback require a new login. Logout deletes
the session, so another replica immediately rejects the same cookie.

For replicas, share an official Redis client between
[`session/redis`](../../session/redis) and [`auth/oauth2redis`](../oauth2redis),
with separate key prefixes. Configure finite timeouts and `MaxRetries: -1` for
non-repeatable `GETDEL`. Memory stores are process-local. Retain secure cookies
in production. Mount CORS and standard-library `http.CrossOriginProtection`
outside Session/login middleware with explicit trusted origins. The application
still owns authorization and account mapping.

Validate this module and its published peers from the contrib root:

```sh
MODULES='auth/login auth/oauth2redis' task check lint vulncheck
```

Requirements and checksums resolve through the public Go proxy without source
replacements. Tests use a locally signed OIDC/JWKS fixture and memory/miniredis
backends; they do not establish external provider acceptance.
