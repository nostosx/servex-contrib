# storage/database/gorm

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

Native GORM integration for servex observability, lifecycle and readiness.
Applications retain `*gorm.DB`, normal transactions and native driver APIs.
There is no parallel CRUD/repository abstraction or automatic schema migration.
Other dialects can use the plugin; backend acceptance currently covers PostgreSQL.

## API

| API | Behavior |
|---|---|
| `NewPlugin(tracer, meter)` | Instruments create/query/update/delete/row/raw callbacks; nil disables each signal |
| `db.Use(plugin)` | Installs once before concurrent use; GORM rejects duplicate plugin names |
| `Hook(name, db)` | Pings on App Start/Check; closes the root connection pool on Stop |
| `DefaultDatabaseConfig()` | Explicit 10-open/2-idle pool defaults, 30-minute lifetime and 5-minute idle expiry |
| `DatabaseConfig.Validate()` | Validates connection syntax and bounded pool settings without network I/O; errors identify fields without exposing credentials |
| `WithGORMConfig(native)` | Injects runtime callbacks, plugins and naming strategy outside the file configuration |
| `OpenPostgres(name, config, tracer, meter, options...)` | Constructs one owned SQL pool plus instrumented native GORM handle; performs no automatic ping or migration |
| `Database.GORM()` / `Database.SQL()` | Native handles sharing exactly the same pool; SQL can be passed directly to Goose |
| `Database.Start` / `Check` / `Stop` | Application lifecycle and readiness, including cleanup before Start |
| `Database.Close` / `Shutdown` | Idempotent construction rollback and DI shutdown aliases of the same pool owner |

Hook reuses the existing SQL lifecycle implementation and preserves error causes.
Give the pool one owner and register its hook before servers/workers, so they stop
first. Transaction handles are rejected. Pool Close follows database/sql behavior
and cannot be interrupted by the Stop context.

## File configuration

`DatabaseConfig` contains only decodable values. Decode over
`DefaultDatabaseConfig()` so omitted pool settings retain their defaults while
explicit zero idle/expiry values remain meaningful. The existing Viper adapter
uses JSON field names and accepts duration strings through its duration hook.

```toml
[database]
max_open_conns = 10
max_idle_conns = 2
conn_max_lifetime = "30m"
conn_max_idle_time = "5m"
```

Supply the DSN through the configured sources, for example
`APP_DATABASE__DSN` with the Viper environment source. The application owns its
top-level Config and chooses this component's configuration as a field; no core
configuration aggregate or global database selection is required.
`DefaultDatabaseConfig()` deliberately leaves DSN empty, and `Validate` rejects
missing credentials/destination settings. Validation errors identify `dsn`,
`max_open_conns`, `max_idle_conns`, `conn_max_lifetime` or `conn_max_idle_time`.

Runtime-only native GORM configuration, Tracer and Meter are injected through
OpenPostgres arguments/options. They are not fields in the serialized config.
`DatabaseConfig` names the database resource; the `OpenPostgres` constructor
continues to identify the actual supported backend.

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
    "github.com/nostosx/servex-contrib/storage/database/gorm"
)

func run(ctx context.Context) (err error) {
    config := gorm.DefaultDatabaseConfig()
    config.DSN = os.Getenv("POSTGRES_DSN")
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
limits through DatabaseConfig before construction. Native GORM options are supplied through
`WithGORMConfig(native)` at construction; the top-level native config and Plugins map are copied,
while plugin/callback objects remain application-owned and immutable. Automatic
ping is always disabled, and a missing GORM logger defaults to Discard. An external
ConnPool or Dialector is rejected by this owning constructor; use NewPlugin/Hook
for an already configured native database instead. MaxOpenConns must be positive;
idle limits must be between zero and the open limit; durations cannot be negative.
DSN parsing errors expose only ErrInvalidDatabaseConfig to avoid leaking credentials.

Start/Check ping the pool with their operation context. A failed Start initiates
owned-pool cleanup itself, because App only rolls back hooks that already started.
Startup retains the ping error and any cleanup error observed within its context;
a later Close joins cleanup that outlives a canceled startup. Stop initiates Close once
and bounds waiting with its context. SQL Close cannot be canceled after it starts;
a later Stop/Close waits for the same cleanup. Close is valid even before Start.
Pass Database.SQL() directly to storage/database/goose.New to share the pool for explicit
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
bash .github/scripts/check-source.sh check lint vulncheck MODULES='storage/database/gorm'
SERVEX_IT_POSTGRES_DSN='host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable' \
    bash .github/scripts/check-source.sh test INTEGRATION=true MODULES='storage/database/gorm'
```

Use a dedicated disposable PostgreSQL instance. The integration test creates and
drops a unique table, exercises CRUD, rollback, readiness and telemetry. The gate
rejects skipped/missing tests. Normal tests use the real PostgreSQL dialect with
sqlmock and require no external service.
