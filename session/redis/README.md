# session/redis

Redis Store for the handwritten `servex-contrib/session` manager. The production
adapter uses the official `github.com/redis/go-redis/v9` v9.23.0 client; the
existing community miniredis fixture is used only in tests. Session itself keeps
its standard-library-only implementation. This module requires Go 1.27.3 and is
MIT licensed.

Released as v1.0.0 with published session v1.0.0. Install with
`go get github.com/nostosx/servex-contrib/session/redis@v1.0.0`.
Release validation uses public dependencies without source replacements.

## Behavior

- `New(client, options...)` accepts an application-owned `*redis.Client` created
  for standalone Redis or Sentinel failover. It performs no network I/O.
- Load/Save/Rotate/Delete implement session.Store. Key namespaces default to
  `servex:session:`; `WithPrefix` isolates applications. Only SHA-256 store keys
  are accepted. Raw cookie tokens are never stored.
- Lua scripts implement insert-only creation, exact version checks, atomic
  rotation and expiring writes. A stale request cannot restore a deleted/rotated
  record. A collision leaves the old session unchanged.
- Versions remain decimal strings in Lua; opaque JSON payloads are not decoded
  or re-encoded there, preserving integers beyond 2^53. Store reads return fresh
  decoded data. Corrupt/inconsistent records fail closed.
- Absolute expiry uses Redis PEXPIREAT. Input deadlines are rounded down to
  milliseconds, never extended. Redis server time rejects expired writes; the
  session manager chooses the earlier absolute/idle deadline. Keep host clocks
  synchronized. The serialized record limit defaults to 128 KiB, configurable
  with WithMaxRecordBytes; align it with the manager's value limit.
- Backend failures retain their causes and pass to the manager's generic error
  handling. There is no fallback to process memory and no extra background worker.

## Usage

This complete example mounts a session-protected HTTPS route. Set REDIS_ADDR and
supply server.crt/server.key. It intentionally does not grant an identity: add
application credential verification and call RenewToken/Put in a login handler.

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "time"

    goredis "github.com/redis/go-redis/v9"

    "github.com/nostosx/servex-contrib/session"
    sessionredis "github.com/nostosx/servex-contrib/session/redis"
)

func run() error {
    client := goredis.NewClient(&goredis.Options{
        Addr: os.Getenv("REDIS_ADDR"), MaxRetries: -1, ContextTimeoutEnabled: true,
        DialTimeout: 3*time.Second, ReadTimeout: 3*time.Second, WriteTimeout: 3*time.Second,
    })
    defer client.Close()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := client.Ping(ctx).Err(); err != nil { return err }
    store, err := sessionredis.New(client, sessionredis.WithPrefix("myapp:session:"))
    if err != nil { return err }
    manager, err := session.New(store, session.WithCookieName("__Host-session"))
    if err != nil { return err }
    mux := http.NewServeMux()
    mux.HandleFunc("GET /me", func(w http.ResponseWriter, r *http.Request) {
        current, _ := session.FromContext(r.Context())
        var user string
        found, err := current.Get("user_id", &user)
        if err != nil { http.Error(w, "session unavailable", 500); return }
        if !found || user == "" { http.Error(w, "authentication required", 401); return }
        w.Header().Set("X-User-ID", user)
        w.WriteHeader(http.StatusNoContent)
    })
    server := &http.Server{
        Addr: "127.0.0.1:8443", ReadHeaderTimeout: 5*time.Second,
        Handler: http.NewCrossOriginProtection().Handler(manager.Middleware(mux)),
    }
    return server.ListenAndServeTLS("server.crt", "server.key")
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

With servex HTTP, mount `httpx.WithHTTPMiddleware(manager.Middleware)`. Configure
credentials, TLS, connection limits and readiness in the application. The caller
owns client Close; share the same prefix/store across replicas.

## Operational boundaries

ClusterClient is not supported: rotating two independently hashed keys requires
one Redis node. No global hash tag is silently added to concentrate all sessions
into one cluster slot. Sentinel clients are accepted, but actual Sentinel
failover is not covered by this module's standalone Redis acceptance.

Disable automatic transport retries for mutations (`MaxRetries: -1`). If a
connection fails after Redis executed a command, the result can be uncertain;
the adapter never retries an application mutation itself. Atomic execution does
not guarantee durable replication: Redis persistence, failover and eviction
policy remain deployment choices. Lost/evicted records require fresh login;
applications requiring durable revocation across failover need stronger storage
guarantees. Do not share the session namespace with unrelated Redis keys.

## Validation

From the repository root, while session is unpublished:

```sh
bash .github/scripts/check-session-redis.sh check lint vulncheck
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 task test INTEGRATION=true MODULES='session/redis'
```

Use a dedicated disposable Redis. Integration creates a unique namespace and
checks the same Store contract as unit tests, multiple managers sharing state,
server-side expiry and stale-write rejection. Unit tests additionally exercise
precision, concurrency, malformed records, cancellation, client failures and
script reload. CI uses the temporary source helper until coordinated publication.
