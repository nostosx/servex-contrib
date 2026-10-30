# jobqueue/asynq

`github.com/nostosx/servex-contrib/jobqueue/asynq` implements the servex
`jobqueue.Enqueuer` and `jobqueue.ReadyRunner` contracts on the Redis-based
task queue [asynq](https://github.com/hibiken/asynq).

- A `jobqueue.Job` maps to an asynq task: `Type` is the task type, `Payload`
  the payload, `ID` the task ID and `Header` the task headers.
- `jobqueue.WithDelay` and `jobqueue.WithMaxRetry` map to asynq's `ProcessIn`
  and `MaxRetry`. `jobqueue.WithUnique` keys are reserved by the Queue itself with Redis
  `SET NX`, so they follow the servex semantics exactly (asynq's own uniqueness
  only covers queue, type and payload).
- Reservations without a positive TTL are reclaimed on the next `Enqueue`
  after the Inspector observes the previous task completed, archived or deleted.
  A handler returning after cancellation, timeout or shutdown cannot release a
  reservation while the processor still retains the task for another attempt.
  The Redis reservation remains until reused; distinct keys that are never
  reused therefore remain stored. Use a positive TTL when bounded retention is
  required. Manually retrying an archived task starts a new lifecycle outside
  the original uniqueness reservation.
- `RunReady` reports readiness once Redis answered and the asynq server is
  processing the queue, so a `jobqueue.NewWorker` hook fails `Start` when
  Redis is unreachable instead of failing the running application later.
- Non-retryable errors fail the job through `asynq.SkipRetry`. A handler panic
  fails the job immediately with a non-retryable `errorx.Internal`
  (reason `ASYNQ_PANIC`).

## Install

Requires Go 1.27.3. Uses published servex v1.4.0.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/jobqueue/asynq@v1.2.0
```

The example version corresponds to the tag `jobqueue/asynq/v1.2.0`;
see [module versioning](../../README.md#versioning).

## Usage

### Periodic enqueueing (v1.3.0)

NewScheduler adds native asynq periodic enqueueing as an app.Hook. The existing
v1.2.0 tag does not contain this API. The following complete program enqueues a
task every minute until interrupted; run a worker for the matching queue/type
separately. The Redis client is borrowed, snapshotted payloads are not mutated,
and stopping the scheduler does not close the application's connection pool.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    sdk "github.com/hibiken/asynq"
    goredis "github.com/redis/go-redis/v9"

    "github.com/nostosx/servex-contrib/jobqueue/asynq"
)

func run() error {
    client := goredis.NewClient(&goredis.Options{Addr:"127.0.0.1:6379",
        DialTimeout:3*time.Second,ReadTimeout:3*time.Second,WriteTimeout:3*time.Second})
    defer client.Close()
    scheduler, err := asynq.NewScheduler(client, &sdk.SchedulerOpts{Location:time.UTC},
        []asynq.Schedule{{Spec:"@every 1m",Type:"refresh",Payload:[]byte(`{}`),
            Options:[]sdk.Option{sdk.Queue("maintenance"),sdk.MaxRetry(3)}}})
    if err != nil { return err }
    if err := scheduler.Start(context.Background()); err != nil { return err }
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()
    <-ctx.Done()
    stop, finish := context.WithTimeout(context.Background(), 5*time.Second)
    defer finish()
    return scheduler.Stop(stop)
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

At most 1024 schedules are accepted, all parsed before startup. Configure SDK
PostEnqueueFunc for enqueue outcomes, short callbacks and finite client timeouts.
One active scheduler is required per schedule set unless the application supplies
leader coordination; multiple replicas otherwise enqueue duplicates. Missed
ticks are not replayed. Enqueued tasks are persisted by Redis according to its
deployment configuration, and worker retries can repeat effects. This adapter
does not promise exactly-once execution.

Stop starts upstream shutdown once and bounds waiting with its context. The SDK
cannot interrupt a callback/cleanup already running; later Stop calls can continue
waiting. Keep the Redis client alive until shutdown finishes, including after a
Stop timeout. Stop-before-Start is terminal. Startup pings Redis before returning
ready. The integration gate verifies an actual periodic enqueue and inspects its
pending task, in addition to the existing queue/worker contracts.

### One-time delayed tasks

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Redis instance at `127.0.0.1:6379`. The program schedules
an email job one minute ahead, then keeps processing the `mail` queue until
Ctrl+C. Wait at least one minute for the handler's log output. The handler
only logs the payload; it sends no email. A second run within the one-hour
uniqueness window returns `jobqueue.ErrDuplicate`. Queue state and uniqueness
keys survive process exit; use a disposable Redis instance for this demo.

```go
package main

import (
	"context"
	"log"
	"time"

	hibiken "github.com/hibiken/asynq"

	"github.com/nostosx/servex-contrib/jobqueue/asynq"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/jobqueue"
)

func sendEmail(_ context.Context, job *jobqueue.Job) (any, error) {
	log.Printf("attempt %d: send %s", job.Attempt, job.Payload)
	return nil, nil // a retryable error schedules another attempt
}

func run() error {
	q, err := asynq.New(hibiken.RedisClientOpt{Addr: "127.0.0.1:6379"}, asynq.WithQueue("mail"))
	if err != nil {
		return err
	}
	defer q.Close()

	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	_, err = q.Enqueue(ctx, &jobqueue.Job{Type: "email.send", Payload: []byte("alice@example.com")},
		jobqueue.WithDelay(time.Minute),
		jobqueue.WithMaxRetry(5),
		jobqueue.WithUnique("welcome:alice", time.Hour),
	)
	if err != nil { // wraps jobqueue.ErrDuplicate while the unique key is reserved
		return err
	}

	worker := jobqueue.NewWorker(q, map[string]endpoint.Endpoint{"email.send": endpoint.Typed(sendEmail)},
		jobqueue.WithName("worker:mail"))
	return app.New(app.WithHooks(worker)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

`NewFromClient(rdb redis.UniversalClient, opts...)` reuses a go-redis client;
`Close` then leaves it open. Tests with miniredis and real Redis cover normal
completion, retry, shutdown, connection loss, task timeouts and admin
cancellation, including late handlers and reservation recovery after terminal
settlement. Final settlement after actual worker lease loss still needs
dedicated verification.

## Options

| Option | Default | Description |
|---|---|---|
| `WithQueue(name)` | `default` | asynq queue used for enqueueing and processing. |
| `WithConcurrency(n)` | 4 | Jobs processed in parallel by `Run`. |
| `WithBackoff(fn)` | 100ms doubling, max 10s | Delay before retrying attempt `attempt`. |
| `WithFailed(fn)` | none | Receives jobs that failed for good and the last error. |
| `WithShutdownTimeout(d)` | 8s | Grace period for running handlers after `Run`'s context ends. |
| `WithHealthCheckInterval(d)` | 15s | Redis check; after three failures `Run` returns a retryable `Unavailable`. |
| `WithLogger(l)` | `nil` (disabled) | Logger of the asynq server (warnings and errors). |
| `WithServerConfig(fn)` | none | Adjusts the generated `asynq.Config`; changing `Queues`, `BaseContext`, `RetryDelayFunc` or `HealthCheckFunc` breaks Queue semantics. |

## Testing

Unit tests use miniredis. Integration tests cover what miniredis does not
support:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).

Logger API in the 2026-10-29 release: pass a core logger.Logger, wrapping an explicitly
selected slog or Zap backend. Nil disables logging; backend adapters live in
logger/slog and logger/zap. Historical published versions accept *slog.Logger.

## Release

Version v1.2.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/jobqueue/asynq@v1.2.0
```
