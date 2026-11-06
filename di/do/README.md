# samber/do lifecycle adapter

Module version for this revision: **v1.3.0**.

Bridge a native `github.com/samber/do/v2` injector to servex lifecycle and health
checks. Register and resolve services with samber/do itself; business constructors
remain ordinary Go functions. This module defines no container or resolver API.

The current source requires servex v1.7.0, Go 1.27.3 and samber/do/v2 v2.1.0.
Earlier releases retain their original API and core requirements. This migration
changes no existing tag and publishes no new version.

## Usage

This complete program resolves a service before starting an application hook.
Deferred cleanup also covers assembly failures and an unstarted App.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "time"

    "github.com/samber/do/v2"

    dox "github.com/nostosx/servex-contrib/di/do"
    "github.com/nostosx/servex/app"
)

type Greeter struct{ message string }

func NewGreeter(message string) *Greeter { return &Greeter{message: message} }

func run(ctx context.Context) (err error) {
    injector := do.New()
    dependencies := dox.Hook("dependencies", injector)
    defer func() {
        stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
        defer cancel()
        if stopErr := dependencies.Stop(stopCtx); stopErr != nil && !errors.Is(err, stopErr) {
            err = errors.Join(err, stopErr)
        }
    }()

    do.ProvideValue(injector, "hello")
    do.Provide(injector, func(i do.Injector) (*Greeter, error) {
        return NewGreeter(do.MustInvoke[string](i)), nil
    })
    service, err := do.Invoke[*Greeter](injector)
    if err != nil { return err }

    worker := app.NewHook("worker", func(context.Context) error {
        fmt.Println(service.message)
        return nil
    }, nil)
    application := app.New(app.WithHooks(dependencies, worker))
    if err := application.Start(ctx); err != nil { return err }
    return application.Stop(ctx)
}

func main() {
    if err := run(context.Background()); err != nil { log.Fatal(err) }
}
```

Keep native `do.Provide`, `do.ProvideValue` and `do.Invoke` calls in `app.go` or
another composition root. Resolve handlers once before serving traffic and inject
their dependencies directly. Even cached container lookups add synchronization
and allocations.

## Lifecycle and ownership

`Hook(name, injector)` returns an `app.Hook` that also implements `health.Checker`.
Register it before servers and workers consuming injector-owned resources, so
servex stops those consumers before shutting down their dependencies.

- Start checks cancellation but constructs no services. Resolve application
  entry points before starting traffic to discover assembly errors.
- Check runs native health checks for the supplied scope and its ancestors.
  It constructs no lazy services and does not check child scopes. Checks through
  this adapter cannot overlap its shutdown and fail after it stops.
- Stop shuts down the supplied scope and children in native dependency order.
  It is idempotent and retains the first result. Individual shutdown errors retain
  service/scope names and remain visible through `errors.Is` and `errors.As`.
- Only services implementing a native do Shutdown interface are cleaned up by do.
  Plain `io.Closer.Close` and `app.Hook.Stop` methods are not discovered. Give each
  resource one lifecycle owner and explicitly adapt its cleanup where required.
- Call the hook's Stop on assembly failure too; servex cannot clean up hooks that
  never started. Use a fresh bounded cleanup context. `Shutdown(ctx, injector)`
  provides direct cleanup when no lifecycle hook was created.

Do not register the owning App in the injector: its health check would recurse
through the dependency hook. Do not register, resolve, health-check or shut down
the native injector concurrently with this hook's shutdown. Both exported
functions panic on a nil injector instead of choosing do's global container.

## Migration from the core DI contract

Servex v1.6.0 removes `servex/di`. This module consequently removes `Container`,
`New` and `ProvideConstructors`; no compatibility container is retained.

| Previous API | Native replacement |
|---|---|
| `dox.New()` | `do.New()` plus `dox.Hook(name, injector)` |
| `di.Supply` | `do.ProvideValue` |
| `di.Provide` / `ProvideConstructors` | `do.Provide` with a typed provider calling the ordinary constructor |
| `di.Resolve` / `di.MustResolve` | `do.Invoke` / `do.MustInvoke` |
| `di.Container` / `di.Resolver` | Native `do.Injector` at the composition root only |
| Container Stop / Shutdown | Lifecycle hook Stop / `dox.Shutdown` |

Native registration and errors belong to samber/do. The old core DI sentinels
and exact-type wrapper behavior are not reproduced. Business packages need no DI
imports, per-package `module.go` or framework-owned provider wrappers.

## Checks

From the repository root:

```sh
MODULES='di/do' task check lint vulncheck
```

Run native assembly and lookup benchmarks from this module, separately from race
tests and other workloads:

```sh
GOWORK=off go test -run='^$' -bench='^Benchmark' -benchmem -count=10 ./...
```

These microbenchmarks do not measure HTTP/gRPC throughput or request latency.

Tests cover lazy provider preservation, readiness, context propagation,
concurrent checks and shutdown, dependency cleanup order, startup rollback and
assembly failure. Executable examples exercise native registration and the bridge.
