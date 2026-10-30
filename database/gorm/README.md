# database/gorm

Native GORM integration for servex observability, lifecycle and readiness.
This independent module is released as v1.0.0, requires Go 1.27.3 and is MIT licensed.
It uses GORM v1.31.2, published servex v1.4.0 and database/sql v1.0.0.
The PostgreSQL resource uses the official GORM PostgreSQL driver v1.6.3 and pgx
v5.10.0. SQL pool ownership, ORM instrumentation and readiness share one object.

Applications retain `*gorm.DB`, normal transactions and native driver APIs.
There is no parallel CRUD/repository abstraction or automatic schema migration.
Other dialects can use the plugin; backend acceptance currently covers PostgreSQL.

## API

| API | Behavior |
|---|---|
| `NewPlugin(tracer, meter)` | Instruments create/query/update/delete/row/raw callbacks; nil disables each signal |
| `db.Use(plugin)` | Installs once before concurrent use; GORM rejects duplicate plugin names |
| `Hook(name, db)` | Pings on App Start/Check; closes the root connection pool on Stop |
| `DefaultPostgresConfig(dsn)` | Explicit 10-open/2-idle pool defaults, 30-minute lifetime and 5-minute idle expiry |
| `OpenPostgres(name, config, tracer, meter)` | Constructs one owned SQL pool plus instrumented native GORM handle; performs no automatic ping or migration |
| `Database.GORM()` / `Database.SQL()` | Native handles sharing exactly the same pool; SQL can be passed directly to Goose |
| `Database.Start` / `Check` / `Stop` | Application lifecycle and readiness, including cleanup before Start |
| `Database.Close` / `Shutdown` | Idempotent construction rollback and DI shutdown aliases of the same pool owner |

Hook reuses the existing SQL lifecycle implementation and preserves error causes.
Give the pool one owner and register its hook before servers/workers, so they stop
first. Transaction handles are rejected. Pool Close follows database/sql behavior
and cannot be interrupted by the Stop context.

## PostgreSQL usage

Set POSTGRES_DSN to an application-configured connection string. This complete
program participates in App lifecycle/readiness until interrupted. It creates no
tables. The resource owns its pool; register it once, not an additional SQL Hook.

```go
package main

import (
    "context"
    "errors"
    "log"
    "os"

    "github.com/nostosx/servex/app"
    "github.com/nostosx/servex-contrib/database/gorm"
)

func run(ctx context.Context) (err error) {
    config := gorm.DefaultPostgresConfig(os.Getenv("POSTGRES_DSN"))
    database, err := gorm.OpenPostgres("postgres", config, nil, nil)
    if err != nil { return err }
    // Also covers construction failures or cancellation before app.Start.
    defer func() { err = errors.Join(err, database.Close()) }()
    // Repositories use database.GORM(); explicit migrations use database.SQL().
    return app.New(app.WithHooks(database)).Run(ctx)
}

func main() {
    ctx, stop := app.SignalContext(context.Background())
    defer stop()
    if err := run(ctx); err != nil { log.Fatal(err) }
}
```

Use `database.GORM().WithContext(ctx)` for queries and transactions. Configure pool
limits through PostgresConfig before construction. Native GORM options may be
supplied through its GORM field; the top-level config and Plugins map are copied,
while plugin/callback objects remain application-owned and immutable. Automatic
ping is always disabled, and a missing GORM logger defaults to Discard. An external
ConnPool or Dialector is rejected by this owning constructor; use NewPlugin/Hook
for an already configured native database instead. MaxOpenConns must be positive;
idle limits must be between zero and the open limit; durations cannot be negative.
DSN parsing errors expose only ErrInvalidPostgresConfig to avoid leaking credentials.

Start/Check ping the pool with their operation context. Stop initiates Close once
and bounds waiting with its context. SQL Close cannot be canceled after it starts;
a later Stop/Close waits for the same cleanup. Close is valid even before Start.
Pass Database.SQL() directly to database/goose.New to share the pool for explicit
migrations; the migrator must not close it or run automatically in each replica.

Do not attach a request context to a global GORM handle. Normal
GORM transaction semantics, ErrRecordNotFound and context errors are preserved.

## Observability

- Spans are named gorm.create/query/update/delete/row/raw, with db.system and
  bounded db.operation attributes. Request context is propagated into callbacks
  and restored afterward; nested operations inherit the active parent span.
- Metrics: `gorm_client_operations_total{system,operation,status}` and
  `gorm_client_operation_duration_seconds{system,operation}`. Distinct names
  separate ORM operations from existing driver-level db_client metrics.
- SQL, parameters, table names, DSNs and raw error messages are excluded. Backend
  failures record a generic error without replacing the caller's GORM error.
  ErrRecordNotFound is not classified as a backend fault. DryRun emits no signals.
- Row spans measure obtaining rows, not later iteration/Scan errors. Explicit
  Begin/Commit/Rollback are outside callbacks; default write transactions are
  within write operations. User hooks that panic may interrupt the SDK callback
  chain; recover at the appropriate application boundary.
- Sessions share callbacks; register once before concurrent use. Discard a handle
  after failed plugin initialization. Avoid installing both ORM and SQL-driver
  instrumentation unless both observation levels are wanted.

The application's GORM logger is separate. Configure it explicitly if SQL/error
logging is allowed; the example disables it to avoid logging parameters.

## Checks

From the repository root:

```sh
task check lint vulncheck MODULES='database/gorm'
SERVEX_IT_POSTGRES_DSN='host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable' \
    task test INTEGRATION=true MODULES='database/gorm'
```

Use a dedicated disposable PostgreSQL instance. The integration test creates and
drops a unique table, exercises CRUD, rollback, readiness and telemetry. The gate
rejects skipped/missing tests. Normal tests use the real PostgreSQL dialect with
sqlmock and require no external service.
