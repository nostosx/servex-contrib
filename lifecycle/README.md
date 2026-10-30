# lifecycle

**v1.0.0 (2026-10-31).** Explicit resource ownership and application assembly cleanup for
servex. Go 1.27.3; MIT. This module depends only on the standard library and
published servex contracts. No SDK is selected and no Close method is discovered
implicitly.

## Usage

This complete program owns one standard-library HTTP client's idle connections.
For a database, Redis client or other SDK, pass its native operations explicitly
in the same way. Resource.Value preserves the native type for ordinary business
constructors. A Scope records ownership during construction, before App.Start.

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    "github.com/nostosx/servex-contrib/lifecycle"
    "github.com/nostosx/servex/app"
)

func run(ctx context.Context) (err error) {
    resources, err := lifecycle.NewScope(10*time.Second)
    if err != nil { return err }
    defer resources.Finish(&err)
    client := &http.Client{Timeout:5*time.Second}
    resource, err := lifecycle.New("http-client",client,lifecycle.Operations[*http.Client]{
        Close:func(_ context.Context, client *http.Client) error {
            client.CloseIdleConnections()
            return nil
        },
    })
    if err != nil { return err }
    if err := resources.Add(resource); err != nil { return err }
    // Pass resource.Value() to constructors that need *http.Client.
    // Register dependent container, servers and workers after resources.
    application := app.New(app.WithHooks(resources),app.WithStopTimeout(10*time.Second))
    return application.Run(ctx)
}

func main() {
    ctx,cancel := app.SignalContext(context.Background())
    defer cancel()
    if err := run(ctx); err != nil { log.Fatal(err) }
}
```

For an SDK whose constructor creates a closable resource, register it immediately
after successful construction. Add transfers ownership only on success; if Add
rejects a duplicate, the caller still owns and must clean up that new object.
Native application resource types such as database/gorm.Database can also be
added directly, provided Stop is idempotent and valid before Start.

## Resource contract

New(name, value, Operations) returns a typed Resource implementing app.Hook,
Check, Shutdown and HealthCheck. Start/Check use the optional Check operation.
Close is invoked once by Stop/Shutdown, including before Start. A nil Close borrows
the value; stopping the wrapper then prevents its further use but leaves the
injected SDK client's ownership with the caller.

Check and Close callbacks must be concurrency-safe with each other. An already
admitted check may overlap cleanup. Checks beginning after Stop do not invoke the
SDK; a check finishing after cleanup begins cannot report ready. Close must attempt
cleanup even when its supplied context has ended. Configure finite SDK I/O
timeouts and honor the callback context where the SDK permits it.

Stop starts the actual cleanup goroutine once. Its context bounds waiting and
does not forcibly interrupt a noncancellable SDK Close. Later Stop calls join the
same cleanup and return its actual result. A completed result takes precedence
over an already canceled repeat caller. Shutdown/HealthCheck bridge do's native
interfaces when do is the sole owner. When Scope owns the Resource, register its
native Value in do instead of giving the same wrapper two lifecycle owners.

## Scope contract

NewScope(timeout) configures a positive cleanup budget. Add accepts named,
non-nil hooks only during assembly; typed nils and duplicate names are rejected.
Start seals the scope and starts resources in registration order. It is one-shot.
Check is ready only after all starts succeed, including for an empty scope;
before startup and after stopping it returns ErrNotReady.

All already constructed resources are released on a failed or canceled Start,
including those whose Start was never reached. Stop cancels an active startup
and waits for its callbacks to exit before closing anything. This prevents an
SDK from starting after it has been closed. Startup callbacks must honor
cancellation; an uncooperative callback can keep cleanup waiting.

Cleanup runs in reverse registration order with a scope-owned context, independent
of the first Stop caller's deadline. Each caller's Stop context bounds only its
own wait. If a hook returns a waiting timeout while its SDK is still closing,
the scope joins its idempotent Stop before releasing the earlier dependency.
The callback receives the configured cleanup deadline, but an SDK Close that
ignores deadlines can continue after it. This preserves dependency order while
allowing callers to stop waiting. Deadlines cannot promise forced termination or
complete cleanup of an uncooperative SDK; use a later Stop to join completion.

Register the scope before dependent containers, servers and workers so app
shutdown stops consumers first. Use `defer scope.Finish(&err)` with a named return
error in the composition root. Finish uses a fresh bounded context even if startup
was canceled or never called; constructor errors and cleanup errors remain
discoverable through errors.Is/As. Repeated cleanup does not duplicate SDK Close
calls or re-add a cleanup error already present in the returned error.

## Validation

From the repository root: `task check lint vulncheck MODULES=lifecycle`.
Race tests cover construction rollback, canceled startup, typed nils, readiness,
concurrent start/stop/check, reverse cleanup and joining after a timeout. Virtual
time verifies that waiting deadlines do not close dependencies ahead of consumers.
Only actual cleanup creates module-owned goroutines; construction creates none.
