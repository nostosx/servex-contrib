# oauth2

**v1.0.0 (2026-10-31).** Authorization-code/PKCE orchestration with the Go project's
official golang.org/x/oauth2 v0.37.0. Go 1.27.3; MIT. Production imports only the
standard library and the official OAuth2 module. Existing auth/oidc remains the
ID-token verifier; neither tokens nor user permissions are silently trusted.

## Complete HTTPS login example

The application configures OIDC_ISSUER, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET,
OAUTH_AUTH_URL, OAUTH_TOKEN_URL and OAUTH_REDIRECT_URL. Configure the provider's
redirect URI to https://localhost:8443/callback and supply server.crt/server.key.
This example verifies a login result; application account mapping and issuing a
persistent logged-in Session belong after VerifyNonce succeeds. It uses the
already published auth/oidc v1.0.0 adapter in addition to this unreleased module.

```go
package main

import (
    "context"
    "crypto/rand"
    "log"
    "net/http"
    "os"
    "time"

    sdk "golang.org/x/oauth2"

    "github.com/nostosx/servex-contrib/auth/oidc"
    "github.com/nostosx/servex-contrib/auth/oauth2"
)

func run() error {
    httpClient := &http.Client{Timeout:5*time.Second}
    id := os.Getenv("OAUTH_CLIENT_ID")
    verifier, err := oidc.New(context.Background(), os.Getenv("OIDC_ISSUER"), id,
        oidc.WithHTTPClient(httpClient))
    if err != nil { return err }
    pending, err := oauth2.NewMemoryStore(10000)
    if err != nil { return err }
    flow, err := oauth2.New(oauth2.Config{OAuth:sdk.Config{
        ClientID:id, ClientSecret:os.Getenv("OAUTH_CLIENT_SECRET"),
        RedirectURL:os.Getenv("OAUTH_REDIRECT_URL"), Scopes:[]string{"openid"},
        Endpoint:sdk.Endpoint{AuthURL:os.Getenv("OAUTH_AUTH_URL"),TokenURL:os.Getenv("OAUTH_TOKEN_URL")},
    }}, pending)
    if err != nil { return err }
    mux := http.NewServeMux()
    mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
        binding := rand.Text()
        authorization, err := flow.Begin(r.Context(), binding)
        if err != nil { http.Error(w,"login unavailable",503); return }
        http.SetCookie(w,&http.Cookie{Name:"__Host-oauth-bind",Value:binding,Path:"/",
            Secure:true,HttpOnly:true,SameSite:http.SameSiteLaxMode,MaxAge:300})
        http.Redirect(w,r,authorization.URL,http.StatusFound)
    })
    mux.HandleFunc("GET /callback", func(w http.ResponseWriter, r *http.Request) {
        cookie, err := r.Cookie("__Host-oauth-bind")
        if err != nil { http.Error(w,"login failed",401); return }
        ctx := context.WithValue(r.Context(), sdk.HTTPClient, httpClient)
        result, err := flow.Complete(ctx,cookie.Value,r.URL.Query().Get("state"),r.URL.Query().Get("code"))
        http.SetCookie(w,&http.Cookie{Name:"__Host-oauth-bind",Path:"/",Secure:true,HttpOnly:true,MaxAge:-1})
        if err != nil { http.Error(w,"login failed",401); return }
        raw, _ := result.Token.Extra("id_token").(string)
        token, err := verifier.VerifyNonce(r.Context(),raw,result.Nonce)
        if err != nil { http.Error(w,"login failed",401); return }
        w.Header().Set("X-Authenticated-Subject",token.Subject)
        w.WriteHeader(http.StatusNoContent)
    })
    server := &http.Server{Addr:"127.0.0.1:8443",ReadHeaderTimeout:5*time.Second,
        Handler:http.NewCrossOriginProtection().Handler(mux)}
    return server.ListenAndServeTLS("server.crt","server.key")
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

When binding to servex Session instead of this example's transient cookie,
persist the binding before redirecting. Rotate the logged-in Session token after
successful identity verification. Never put provider access/refresh tokens in
plain cookies or logs. The example handles query-based callbacks; providers using
form_post require an explicitly designed cookie/CSRF callback policy.

## State, tokens and replicas

Begin stores a random state's SHA-256 key, PKCE verifier, nonce, expiry and hashed
browser binding server-side. Complete atomically consumes state before exchange,
checks binding/expiry and uses S256 PKCE. State TTL defaults to five minutes and
is restricted to one second through ten minutes. Failed/uncertain exchanges
require a fresh login, not replay. Do not log provider response bodies from SDK
errors; they can contain sensitive data. Return generic login errors to browsers.

StateStore is an atomic insert/consume contract. MemoryStore is bounded, lazily
expires entries and loses pending logins on restart. It is suitable for one process;
multiple replicas require an application-supplied shared StateStore. This module
does not claim that its memory state survives restarts or works across replicas.

TokenSource refreshes using SDK semantics; its context must outlive refreshes.
Persist refreshed tokens explicitly when needed. ClientCredentials accepts the
SDK's clientcredentials.Config for machine-to-machine tokens. HTTPS is required;
AllowInsecureLoopback permits only HTTP localhost or loopback IP literals in the
authorization-code client's local-development configuration. Endpoints are trusted
application configuration; set a finite-timeout, restricted HTTP client through
oauth2.HTTPClient for token exchange and refresh.

From the root: `task check lint vulncheck MODULES=auth/oauth2`. Local HTTP token
fixtures verify real code exchange and the PKCE challenge, browser binding,
single consumption, expiry, capacity and cancellation. No live identity provider
or account provisioning is part of ordinary acceptance.
