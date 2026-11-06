# Fx lifecycle adapter

Module version for this revision: **v1.0.0**.

The recommended dependency assembly for servex uses native `go.uber.org/fx` in
application `app.go`, with ordinary business constructors. This module connects
that native Fx application to servex lifecycle, readiness and runtime shutdown.
It defines no container, provider or dependency-resolution API.

Current source uses Go 1.27.3, published servex v1.7.0 and Fx v1.24.0. This new
contrib module is not yet published. No local replacements are checked in.

## Explicit assembly

This complete program constructs a service using native Fx, runs it through a
servex hook, and requests a clean shutdown through native `fx.Shutdowner`.
Replace the demonstration hook with the servers/workers built by the application.

```go
package main

import (
    "context"
    "fmt"
    "log"

    "go.uber.org/fx"

    fxbridge "github.com/nostosx/servex-contrib/di/fx"
    "github.com/nostosx/servex/app"
)

type Greeter struct{ message string }
func NewGreeter(message string) *Greeter { return &Greeter{message: message} }

func run(ctx context.Context) error {
    var service *Greeter
    var shutdowner fx.Shutdowner
    native := fx.New(
        fx.NopLogger,
        fx.Supply("hello"),
        fx.Provide(NewGreeter),
        fx.Populate(&service, &shutdowner),
    )
    dependencies := fxbridge.Hook("dependencies", native)
    worker := app.NewHook("worker", func(context.Context) error {
        fmt.Println(service.message)
        return shutdowner.Shutdown()
    }, nil)
    application := app.New(app.WithHooks(dependencies, worker))
    return dependencies.Run(ctx, application)
}

func main() {
    if err := run(context.Background()); err != nil { log.Fatal(err) }
}
```

Register the bridge **before** servers and workers that consume its resources.
Those consumers drain and stop before Fx closes their dependencies. Construct
middleware and declare its order in the application; Fx does not install any AOP
policy. Keep `fx.Provide`, `fx.Invoke`, `fx.Populate` and optional `fx.Module`
choices at the composition root. A `module.go` in every business package is not
required.

## Lifecycle and ownership

`Hook(name, native)` returns a `*Bridge` implementing `app.Hook`, `health.Checker`
and the optional `Err() <-chan error` runtime failure interface. Construction
starts no work and subscribes to no signals.

- Start delegates to native Fx startup with the configured `StartTimeout` and
  the caller context. Fx executes native hooks and rolls back completed starts
  after a failure. The bridge also finishes native cleanup on a failed startup.
- Readiness reports the bridge running state and native initialization errors.
  Fx exposes no application health-check API. Register resource-specific servex
  readiness checkers separately, with one lifecycle owner per resource.
- Stop starts one cleanup operation with the native `StopTimeout`, independently
  of the first caller cancellation. Each caller can stop waiting; later calls
  join the same result. Native hooks must honor their contexts. A deadline cannot
  forcibly terminate an uncooperative SDK operation.
- Owned resources should be acquired in native OnStart and released in OnStop.
  Fx initialization invokes constructors before startup; it cannot automatically
  clean arbitrary resources acquired by constructors that later fail assembly.

`Bridge.Run(ctx, application)` requires the explicitly assembled application to
contain this bridge. It calls servex Run and forwards native Fx shutdown requests:

- Fx Wait is the **only OS signal receiver**. Do not also call `fx.App.Run`,
  `fx.App.Wait`/`Done` or `app.SignalContext` for that application.
- After startup, a normal Shutdowner request or parent cancellation stops servex
  cleanly. Cancellation during startup aborts it and returns the startup error.
- `Shutdown(fx.ExitCode(n))` with nonzero n reports `*ExitError` through the
  bridge runtime channel and retains that error in Run's result. Use `errors.As`
  to choose the process exit code. The library never calls `os.Exit`.
- servex retains readiness, runtime failure handling, drain and reverse shutdown.
  The Fx signal receiver and forwarding goroutine are stopped and joined on exit.

Using only Hook with direct servex Start/Stop is valid for externally managed
lifecycles, but does not listen to Fx Shutdowner or OS signals. Use Bridge.Run
for the recommended long-running application path.

## Event logging

Pass `fx.WithLogger(func() fxevent.Logger { return fxbridge.NewLogger(log) })`
to native Fx. Here `log` implements the core logger contract and `fxevent` is
`go.uber.org/fx/fxevent`. Nil logging disables Fx events. The adapter emits native
lifecycle/function identities and safe error classifications without error text,
causes, stack traces, module names or provided values. It does not choose a logger
backend or fabricate request trace IDs.

## Validation

From this module, use the public core and SDK directly:

```sh
task --dir ../.. check lint vulncheck MODULES=di/fx
```

The regression suite covers native ordering and drain, readiness, initialization
failure, startup rollback, cancellation and budgets, independent Stop waiters,
normal/nonzero Shutdowner requests, servex runtime failures and safe native event
logging. No external service is required.
