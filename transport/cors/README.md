# cors

**Unreleased.** Explicit HTTP browser-origin policy using community rs/cors
v1.11.1 (MIT). Go 1.27.3. Independent SDK module; generic middleware remains
SDK-free. Callbacks/options retain their upstream meanings.

## Usage and standard-library CSRF protection

This complete HTTPS server handles preflight outside routing and rejects unsafe
browser requests from untrusted origins using Go's CrossOriginProtection.
Supply server.crt/server.key. Replace the application origin with your own.

```go
package main

import (
    "log"
    "net/http"
    "time"

    sdk "github.com/rs/cors"

    "github.com/nostosx/servex-contrib/transport/cors"
)

func run() error {
    protect := http.NewCrossOriginProtection()
    if err := protect.AddTrustedOrigin("https://app.example"); err != nil { return err }
    wrap, err := cors.New(sdk.Options{
        AllowedOrigins: []string{"https://app.example"},
        AllowedMethods: []string{http.MethodGet, http.MethodPost},
        AllowedHeaders: []string{"Content-Type"},
        AllowCredentials: true,
    })
    if err != nil { return err }
    mux := http.NewServeMux()
    mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusNoContent)
    })
    server := &http.Server{Addr:"127.0.0.1:8443", ReadHeaderTimeout:5*time.Second,
        Handler:wrap(protect.Handler(mux))}
    return server.ListenAndServeTLS("server.crt", "server.key")
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

For servex HTTP mount these through httpx.WithHTTPMiddleware: CORS must be outer
to CSRF and authentication so OPTIONS can complete. Session middleware still
requires secure cookies and application authentication. CORS controls browser
response access, not who can call the API. CSRF trusted origins are a separate
explicit trust decision. Keep GET/HEAD/OPTIONS free of state changes.

New rejects an absent origin policy, empty origins and credentials combined with
the universal wildcard. Origin/method/header slices are snapshotted. Origin
callbacks are application-owned and must be concurrency-safe. Upstream rejects
malformed preflight header lists; real browsers send lowercase names there.

From the repository root: `task check lint vulncheck MODULES=transport/cors`.
Preflight tests check credentials, origin rejection, early termination and
configuration isolation. transport/httpx also tests standard-library CSRF
composition. Neither module adds a separate third-party CSRF dependency.
