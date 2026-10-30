# etcd coordination

**Unreleased.** Lease-backed locks and leader election using the official
go.etcd.io/etcd/client/v3/concurrency v3.7.3. Go 1.27.3; MIT. The official
embedded server is a test-only dependency. No Redis lock algorithm is added.

## Usage

This complete program acquires a namespaced lock and prints its fencing revision.
Supply ETCD_ENDPOINTS as a comma-separated endpoint list. Configure authenticated
TLS in the SDK client when connecting outside a disposable local environment.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "strings"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"

    "github.com/nostosx/servex-contrib/coordination/etcd"
)

func run() error {
    client, err := clientv3.New(clientv3.Config{
        Endpoints:strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","), DialTimeout:3*time.Second,
    })
    if err != nil { return err }
    defer client.Close()
    coordinator, err := etcd.New(client, "/myapp/", 10)
    if err != nil { return err }
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()
    lock, err := coordinator.Acquire(ctx, "refresh")
    if err != nil { return err }
    defer func() {
        cleanup, finish := context.WithTimeout(context.Background(), 3*time.Second)
        defer finish()
        if err := lock.Release(cleanup); err != nil { log.Print(err) }
    }()
    fmt.Println("fencing revision:", lock.Revision)
    // Enforce this revision in the protected storage operation before writing.
    return nil
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

Campaign(ctx, name, value) returns Leadership after election. Call Resign(ctx)
on completion. Locks and elections use separate subnamespaces. Names are single
path segments (up to 256 bytes); values are bounded to 4096 bytes. TTL is 1..86400
seconds. Construction starts no goroutines or network I/O; each acquisition starts
one SDK-managed lease keepalive and must be released. The caller owns the client.

## Ownership and failure

The acquisition context also owns the full session lifetime: do not cancel a
short-lived acquisition context and expect to retain ownership afterward. Done
closes when the session ends or stops refreshing. Release/Resign are idempotent,
stop keepalive and attempt server-side revocation. If cancellation prevents
revocation, the lease can remain until its TTL elapses; do not claim immediate
transfer of ownership after an unsuccessful release. SDK outage cleanup can also
outlive an operation's deadline; client lifecycle must be bounded by the application.

Enforce Lock.Revision or Leadership.Revision as a fencing token in external
protected resources. Watching Done and cancellation alone cannot prevent a paused
former owner from writing later. Revisions are valid within one etcd cluster's
history, not a global token across replaced/restored clusters.

## Checks

`task check lint vulncheck MODULES=coordination/etcd` from the root. Ordinary tests
start an owned embedded etcd. External acceptance uses a uniquely named prefix
against a disposable server:

```sh
SERVEX_IT_ETCD_ENDPOINTS=127.0.0.1:2379 task test INTEGRATION=true MODULES=coordination/etcd
```

Both paths observe the contender entering etcd, verify mutual exclusion, transfer
and increasing revisions, then exercise election, lease shutdown and cancellation.
They do not certify multi-node failover or production cluster configuration.
