# cron

**Unreleased.** In-process scheduling with community robfig/cron/v3 v3.0.1
(MIT), Go 1.27.3. Implements servex app.Hook. No Redis or durable storage is
required. The upstream version is mature but has a slow release cadence.

## Usage

This complete program starts one periodic job and stops on a termination signal.
The same scheduler can be registered as an app.Hook in an application's assembly.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/nostosx/servex-contrib/scheduler/cron"
)

func run() error {
    scheduler, err := cron.New(cron.Config{Jobs:[]cron.Job{{
        Name:"refresh", Spec:"@every 1m", Timeout:10*time.Second,
        Run:func(ctx context.Context) error { return ctx.Err() },
    }}, OnError:func(name string, err error) { log.Printf("job %s: %v", name, err) }})
    if err != nil { return err }
    if err := scheduler.Start(context.Background()); err != nil { return err }
    signals, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()
    <-signals.Done()
    stop, finish := context.WithTimeout(context.Background(), 5*time.Second)
    defer finish()
    return scheduler.Stop(stop)
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

## Behavior

New parses five-field cron expressions/descriptors and rejects invalid jobs,
duplicate names, absent functions and nonpositive execution timeouts. UTC is the
default; Config.Location or CRON_TZ expressions select the time zone. At most
1024 jobs may be configured. Every job is serialized with skip-if-busy behavior;
different jobs run independently. Errors/panics reach OnError. Callbacks must be
safe for concurrent use and return promptly without panicking.

Start creates an owned run context, retaining startup values but not its deadline
or cancellation. Stop first prevents ticks and cancels jobs, then waits within
its own deadline. A timed-out Stop does not forcibly terminate an uncooperative
job; later Stop calls may continue waiting. The scheduler is one-shot, including
Stop-before-Start. No implicit global scheduler is created.

Schedules are process-local. Multiple replicas execute independently; missed
runs are not persisted/replayed. Use the existing jobqueue/asynq periodic
scheduler for durable enqueueing, with one scheduler instance per schedule set
or explicit leader coordination. Neither option promises exactly-once effects.

From the root: `task check lint vulncheck MODULES=scheduler/cron`. Tests use Go's
virtual-time synctest for real ticks, overlap suppression, cancellation, panic
handling and lifecycle. No wall-clock sleeps are needed for this module's tests.
