# samber/do dependency injection

Implement the core `di.Container` contract with samber/do/v2. Register and
resolve dependencies with `github.com/nostosx/servex/di`; this module connects
container cleanup and health checks to servex. Constructors
remain ordinary Go functions. Keep registration in one application entry point
or `app.go`; neither do nor Fx requires a `module.go` in each package.

Version v1.1.0 requires servex v1.4.0, Go 1.27.3 and samber/do/v2 v2.1.0.
Install it in the application module; no sibling checkout or local replacement
is required:

```sh
go get github.com/nostosx/servex@v1.4.0 github.com/nostosx/servex-contrib/di/do@v1.1.0
```

## API and ownership

```go
func New() *Container // implements di.Container, app.Hook and health.Checker

// Optional bridge for an existing native do.Injector:
func Hook(name string, injector do.Injector) app.Hook
func Shutdown(ctx context.Context, injector do.Injector) error
```

- `di.Provide[T]` registers a lazy singleton; `di.Supply[T]` registers and
  resolves an existing value immediately so it is owned even if unused.
  `di.Resolve[T]` resolves the exact type; register interface bindings as that
  interface explicitly. Duplicate registrations keep the original provider.
  Failed constructors may retry. Missing dependencies, duplicates, cycles,
  closed containers and invalid arguments/results wrap the core DI sentinels.
- Use the provider's `di.Resolver` for dependencies. It records the resolution
  path; do not retain it, capture the container for lookups, or register services
  inside a provider. Root resolution is serialized to prevent concurrent cyclic
  graphs from deadlocking on do's singleton locks.
- Resolve handlers/services once during assembly, then inject those values into
  request handlers. The resulting endpoint has no container lookup in its
  request path. Even a cached Resolve takes locks and enters samber/do's
  invocation machinery; do not use the container as a per-request service
  locator. See [performance measurements](PERFORMANCE.md) for the benchmark
  scope, results and reproduction command.
- `Container` is itself a hook named `di`; use `app.WithHooks(container, server)`.
  It shares the lifecycle behavior described below for the native bridge.
- `Hook.Start` does not resolve providers or start services. Resolve the root
  services before calling `app.Start` or `app.Run`; constructor failures then
  occur before the application accepts traffic. A canceled start returns the
  context error.
- Register the injector hook **first**, then servers and workers. servex stops
  those consumers before do shuts down their dependencies. The same order
  applies to rollback after a later hook fails to start.
- The hook also implements `health.Checker`. It delegates to do's
  `HealthCheckWithContext`, checks already constructed services implementing
  do's health interfaces, and leaves unused lazy providers untouched. Like
  do's API, health checks cover the supplied scope and its ancestors, not
  child scopes. It is not validation of the complete registered graph.
- Shutdown includes child scopes and follows do's dependency ordering. Errors
  include service/scope names and preserve their causes through `errors.Is`
  and `errors.As`. A successful shutdown returns a genuine nil error, rather
  than a non-nil `*do.ShutdownReport` whose message happens to be empty.
  Hook `Stop` is idempotent and preserves its first result. Checks through
  that hook cannot overlap shutdown and fail after the hook stops.
- do recognizes its `Shutdown` interfaces, including
  `Shutdown(context.Context) error`. It does **not** automatically call
  `Close`, servex `Stop`, or servex `Check`. Wrap external resources explicitly
  or give them their own servex hooks. Transient instances remain the caller's
  responsibility.
- Give each resource one lifecycle owner. The example below gives servex
  ownership of the HTTP server; do only constructs that server. Never register
  the owning `*app.App` in this injector: its `Check` would recurse through the
  container hook. Do not shut down or health-check the injector directly
  concurrently with this hook; do's own lifecycle API is not synchronized by
  the adapter. Stop application work before releasing its dependencies.
- Call the hook's `Stop` on assembly failure too. servex does not stop hooks that have
  never started, including when the initial context is already canceled.
  The deferred cleanup below covers these paths and is safe after servex has
  already stopped the hook. `Shutdown` is also available for direct cleanup
  when no hook has been created.
  Do not register or resolve new services after shutdown begins.
- Both functions panic on a nil injector instead of using do's global default.

## Plain constructor registration (v1.2.0)

ProvideConstructors removes provider-closure boilerplate while keeping business
constructors independent of the container. It is a new source API, not part of
the published v1.1.0 tag. With the ordinary Config, NewGreeter and NewServer
constructors in the full example below, registration becomes:

```go
if err := di.Supply(container, cfg); err != nil { return err }
if err := dox.ProvideConstructors(container, NewGreeter, NewServer); err != nil {
    return err
}
server, err := di.Resolve[*httpx.Server](container)
if err != nil { return err }
```

This is an assembly fragment. Accepted functions return T or (T, error), with
fixed input parameters. Signature validation and duplicate output checks cover
the entire batch before registration begins. Container registration failures can
leave earlier registrations installed; no constructor executes during registration.
Dependencies and outputs use exact declared types. Bind an interface with a
constructor returning that interface; a concrete implementation is not inferred
as an interface registration. Variadic constructors need an explicit wrapper
that supplies their options.

Reflection is confined to construction of the dependency graph. Resolve root
services once before serving requests and pass ordinary typed pointers to the
handlers. Constructor errors preserve their causes. ProvideConstructors adds no
implicit lifecycle ownership, Close discovery or per-package registration files;
the lifecycle rules above continue to apply.

## One entry point with do

This is a complete application. Save it as `main.go` in a separate consumer
module with the released dependencies above, then run `go mod tidy` and
`go run .`. It serves `GET /greeting` on loopback port
8080 until Ctrl+C. There are no external services or per-package module files.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log/slog"
    "net/http"
    "os"
    "time"

    dox "github.com/nostosx/servex-contrib/di/do"
    "github.com/nostosx/servex/app"
    "github.com/nostosx/servex/di"
    "github.com/nostosx/servex-contrib/transport/httpx"
)

type Config struct {
    Address string
    Message string
}

type Greeter struct{ message string }

func NewGreeter(cfg Config) *Greeter {
    return &Greeter{message: cfg.Message}
}

func NewServer(cfg Config, greeter *Greeter) *httpx.Server {
    server := httpx.NewServer(httpx.WithAddress(cfg.Address))
    server.HandleHTTP("GET /greeting", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if _, err := fmt.Fprintln(w, greeter.message); err != nil {
            slog.ErrorContext(r.Context(), "write greeting", "error", err)
        }
    }))
    return server
}

func run(ctx context.Context) (err error) {
    container := dox.New()
    var registry di.Container = container
    // Also release dependencies if assembly fails or Start never runs.
    defer func() {
        stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
        defer cancel()
        if stopErr := container.Stop(stopCtx); stopErr != nil && !errors.Is(err, stopErr) {
            err = errors.Join(err, stopErr)
        }
    }()

    if err := di.Supply(registry, Config{Address: "127.0.0.1:8080", Message: "hello"}); err != nil {
        return err
    }
    if err := di.Provide(registry, func(r di.Resolver) (*Greeter, error) {
        return NewGreeter(di.MustResolve[Config](r)), nil
    }); err != nil {
        return err
    }
    if err := di.Provide(registry, func(r di.Resolver) (*httpx.Server, error) {
        return NewServer(di.MustResolve[Config](r), di.MustResolve[*Greeter](r)), nil
    }); err != nil {
        return err
    }

    // MustResolve inside a provider is recovered by the outer Resolve.
    server, err := di.Resolve[*httpx.Server](registry)
    if err != nil {
        return err
    }
    return app.New(
        app.WithHooks(container, server),
        app.WithStopTimeout(10*time.Second),
    ).Run(ctx)
}

func main() {
    ctx, cancel := app.SignalContext(context.Background())
    defer cancel()
    if err := run(ctx); err != nil {
        slog.Error("application failed", "error", err)
        os.Exit(1)
    }
}
```

Use ordinary constructors inside application/domain packages. The provider
closures above belong to the entry point; do not change `NewGreeter` to accept
an injector or store an injector in business structs. If a constructor can
fail, return its error directly from its provider.

## The same assembly with Fx

Keep the same `Config`, `NewGreeter` and `NewServer`. Import `go.uber.org/fx`,
remove the unused `dox`, `di` and `errors` imports,
and replace `run` with the following. The consumer needs `go.uber.org/fx`; this adapter
does not depend on Fx. Both versions let servex own server startup, readiness,
runtime failure handling and shutdown. This comparison uses Fx only for
construction, since servex already provides the application lifecycle.

```go
func run(ctx context.Context) error {
    var server *httpx.Server
    container := fx.New(
        fx.Supply(Config{Address: "127.0.0.1:8080", Message: "hello"}),
        fx.Provide(NewGreeter, NewServer),
        fx.Populate(&server),
    )
    if err := container.Err(); err != nil {
        return err
    }
    return app.New(
        app.WithHooks(server),
        app.WithStopTimeout(10*time.Second),
    ).Run(ctx)
}
```

Here the constructors register no Fx lifecycle hooks and allocate no resources
needing Fx cleanup. If you add Fx hooks, drive `container.Start`/`Stop` too,
and choose one owner for signals and runtime failures. Fx invokes/populates
dependencies during `fx.New`; `Start` runs the registered lifecycle hooks.

| Concern | Fx | do/v2 |
|---|---|---|
| Constructor registration | `fx.Provide(NewGreeter, NewServer)` | Typed provider closures that call ordinary constructors |
| Dependency resolution | Constructor parameters inspected by reflection | Explicit `di.MustResolve[T]` in provider closures |
| Trigger construction | `fx.Invoke` or `fx.Populate` during `fx.New` | `di.Resolve[T]` at the composition root |
| Missing dependency | Error when the relevant graph is resolved | Error when the relevant graph is resolved; generics do not prove registration completeness |
| Lifecycle with servex | Choose an owner and bridge Fx hooks when used | servex hooks plus this adapter for do-owned dependencies |
| Package organization | `fx.Module` is optional | `do.Package` is optional |
| Business constructors | Ordinary Go functions | The same ordinary Go functions |

Choose based on registration ergonomics and lifecycle needs, not a filename
convention. Fx has shorter constructor registration. do makes each dependency
lookup explicit and fits a servex-owned lifecycle through this small adapter.

The core contract uses exact type keys and provider functions. Backend features
such as do scopes and named services remain available through the native bridge.
The Fx comparison uses Fx directly; this module implements the core contract
only for do, and does not add an Fx backend.

## Checks

From the repository root:

```sh
MODULES='di/do' task check lint vulncheck
task test
```

## Release

Version v1.1.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/di/do@v1.1.0
```
