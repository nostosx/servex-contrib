# session

Server-side browser sessions implemented directly with the Go standard library.
No SCS or other session SDK is used. This independent module is released as v1.0.0,
requires Go 1.27.3 and is MIT licensed.

## API and defaults

- `New(store, opts...)` constructs an immutable manager with an explicit Store.
- `manager.Middleware(handler)` loads and persists each request's session.
- `FromContext(ctx)` retrieves a request-owned Session. Put/Get use JSON values;
  Get decodes into an application destination without aliasing stored data.
- `RenewToken` atomically replaces the token, keeping values and the original
  absolute deadline. `Destroy` revokes the server record and expires the cookie.
- `NewMemoryStore(capacity)` constructs a bounded process-local store. It uses
  lazy expiry and starts no cleanup goroutines; live records are never evicted.

Defaults: host-only cookie named session, path /, HttpOnly, Secure, SameSite=Lax;
24-hour absolute expiry, 30-minute inactivity expiry, 64 KiB JSON values, and no
cookie persistence across browser exit. Tokens contain 32 cryptographically random
bytes encoded with base64url. Only their SHA-256 hashes reach the Store.

Options: WithCookieName, WithLifetime, WithIdleTimeout, WithSameSite,
WithInsecureCookies (local HTTP), WithPersistentCookie, WithMaxDataBytes and
WithErrorHandler. Zero idle timeout disables it. SameSite=None and secure cookie
prefixes require Secure. Persistent cookies retain the absolute deadline; idle
expiry is enforced server-side. Invalid or unknown tokens become anonymous and
their cookies are cleared. Duplicate session cookies produce HTTP 400.

## Usage

This complete program demonstrates reading and destroying sessions. Supply
server.crt/server.key before running. Authenticate credentials in the login
handler before assigning a user ID; the example rejects login until implemented.

```go
package main

import (
    "log"
    "net/http"
    "time"

    "github.com/nostosx/servex-contrib/session"
)

func main() {
    store, err := session.NewMemoryStore(10000)
    if err != nil { log.Fatal(err) }
    manager, err := session.New(store, session.WithCookieName("__Host-session"))
    if err != nil { log.Fatal(err) }
    mux := http.NewServeMux()
    mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
        http.Error(w, "authentication required", http.StatusUnauthorized)
    })
    mux.HandleFunc("GET /me", func(w http.ResponseWriter, r *http.Request) {
        current, _ := session.FromContext(r.Context())
        var userID string
        found, err := current.Get("user_id", &userID)
        if err != nil { http.Error(w, "session unavailable", 500); return }
        if !found || userID == "" { http.Error(w, "authentication required", 401); return }
        w.Header().Set("X-User-ID", userID)
        w.WriteHeader(http.StatusNoContent)
    })
    mux.HandleFunc("POST /logout", func(w http.ResponseWriter, r *http.Request) {
        current, _ := session.FromContext(r.Context())
        if err := current.Destroy(); err != nil { http.Error(w, "session unavailable", 500); return }
        w.WriteHeader(http.StatusNoContent)
    })
    server := &http.Server{
        Addr: "127.0.0.1:8443",
        Handler: http.NewCrossOriginProtection().Handler(manager.Middleware(mux)),
        ReadHeaderTimeout: 5*time.Second,
    }
    log.Fatal(server.ListenAndServeTLS("server.crt", "server.key"))
}
```

After credential verification, call `current.RenewToken()` and
`current.Put("user_id", authenticatedUserID)`, handling both errors, before
writing a response. Mount with `httpx.WithHTTPMiddleware(manager.Middleware)`
when using servex HTTP. Applications own authentication, authorization and CSRF
policy. The example uses standard-library same-origin protection.

## Persistence and concurrency

The Store contract requires atomic version checks for Save and Rotate. Creating
a record uses expected version zero; updates require a live matching version.
Deleted, rotated and expired records cannot be recreated by stale requests.
Concurrent changed snapshots conflict with HTTP 409; applications decide whether
their business operation is safe to retry. Idle-only refresh conflicts are ignored
so concurrent reads never restore revoked records or overwrite newer values.

Session changes persist before the first write/flush or on normal handler return.
A persistence error suppresses the handler body and issues a generic HTTP 500
(409 for conflicts, 503 for capacity). Handler business effects are not rolled
back. Custom error handlers own formatting/logging and must write a failure
response. Finish all session mutations before writes; late mutations fail with
ErrCommitted, and retaining a Session beyond its request yields ErrClosed.
Panicking handlers do not commit unless they already wrote/flushed a response.

ResponseController flushing and hijacking are supported through the original
writer. Hijacking with pending value/token/cookie changes is rejected; establish
the browser session in a prior HTTP request before authenticating an upgrade.
Unsupported writer capabilities return http.ErrNotSupported.

MemoryStore loses sessions on restart and cannot share them across replicas.
For production shared storage, use the independent [Redis adapter](redis/README.md),
which owns the official Redis SDK and atomic persistence scripts. It currently
supports standalone/Sentinel clients; it is released independently with this module.
Its capacity bounds stored records, not simultaneous request snapshots. Expired
entries are removed on access/capacity pressure. A shared Redis/SQL implementation
must honor the documented atomic Store contract. Redis lives in a separate module.
The manager never closes an injected store. Raw tokens and session values should
not be logged. This module provides no compatibility with SCS session records.

## Checks

```sh
task check lint vulncheck MODULES='session'
```

Tests cover lifecycle, TLS cookie roundtrips, random/hashed tokens, expiry with
virtual time, failed-persistence body suppression, concurrent revocation/CAS,
record isolation, capacity, cancellation and response commitment. These are Go
HTTP tests, not real-browser automation.
