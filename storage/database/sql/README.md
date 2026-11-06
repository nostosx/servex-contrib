# storage/database/sql

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../../CONTRIBUTING.md#checks).

Business transaction scopes are available through
`NewTransactor(db, options)`. Inject the same scope into transaction middleware
and participating repositories. `Within(ctx, callback)` runs once with a
transaction-bearing context; `scope.Executor(ctx)` returns that transaction's
native query/exec interface, otherwise the borrowed pool. A repository must pass
the callback context and use Executor, rather than directly using the pool.
No pool ownership, retry or migration is implied.

Nested calls on the same scope return `ErrNestedTransaction`; propagate that
error to roll back the outer transaction. Independent scopes do not provide
distributed atomicity. Callback contexts/executors must not escape the call.
Completed transactions fail with `sql.ErrTxDone`, without falling back to the
pool. `middleware/application.Transaction` accepts the scope structurally, so this SQL
adapter has no dependency on middleware. See [application handlers](https://github.com/nostosx/servex#typed-application-handlers) for assembly.

database/sql adapter.

`NewPostgresIdempotency(scope)` implements durable keyed execution.
Run `EnsureSchema(ctx)` explicitly in the migration workflow. PostgreSQL row
locks serialize keys; the success record and business writes using the same
`scope.Executor(ctx)` commit atomically. Matching completed calls replay encoded
results and changed digests return ErrConflict. Records have no automatic expiry.
Keys require a trusted scope, operation, token and semantic fingerprint.

The store owns the transaction. An inner business transaction decorator can use
`applicationmw.TransactionRunnerFunc(scope.Join)` explicitly, where applicationmw
is contrib middleware/application; ordinary `Within` still rejects
accidental nesting. Remote effects and other database scopes are not atomic with
the record. Reconcile an uncertain commit by retrying the same key.

Package database wraps a caller-selected database/sql driver/Connector for
tracing, metrics and lifecycle/readiness. It returns native *sql.DB values.
There is no new database abstraction or ORM. No-op telemetry defaults live
in contrib; existing optional driver capabilities and error identities are preserved.

## File configuration

`DatabaseConfig` is a data-only configuration for one driver-selected SQL pool:

```toml
[database]
driver = "pgx"
max_open_conns = 10
max_idle_conns = 2
conn_max_lifetime = "30m"
conn_max_idle_time = "5m"
```

The application imports/registers its chosen SQL driver and supplies DSN through
its configured sources, for example `APP_DATABASE__DSN`. This module does not
choose or import a driver SDK. Start with `DefaultDatabaseConfig()` and decode
JSON-tagged fields over it. Omitted values retain defaults; explicit zero idle
connections or expiry durations disable those policies. The Viper adapter's
Unmarshal supports duration strings.

With an application-owned configuration snapshot and telemetry dependencies:

```go
cfg := database.DefaultDatabaseConfig()
if err := view.Unmarshal("database", &cfg); err != nil { return err }
db, err := database.OpenFromConfig(cfg,
    database.WithTracer(tracer), database.WithMeter(meter),
)
if err != nil { return err }
// Register exactly one lifecycle owner before consumers start.
owner := database.Hook("database", db)
```

This is an assembly fragment; `view` is a core config.Reader and driver import,
tracer, meter and owner registration belong to the application. `Validate()`
checks required fields and pool bounds before driver construction. It does not
validate backend-specific DSN syntax or require a driver to have been registered.
`OpenFromConfig` applies all pool settings without pinging; construction errors
omit driver error text while preserving `errors.Is`/`As` through the cause.
Invalid settings match `ErrInvalidDatabaseConfig`. Start/Check readiness remains
explicit through `Hook`. A failed Hook Start closes its own pool and joins ping
and cleanup errors, since App rolls back only previously started hooks. Later
readiness failures do not close a running pool. `db.Close` releases an unstarted
pool on assembly failure.

Use the existing `Open(driver, dsn, options...)` or `OpenDB(connector, options...)`
when the application already assembles native options or a driver connector.
All constructors return native `*sql.DB`; there is no extra SQL facade or
implicit schema migration.

## Validation

Run repository `task check MODULES='storage/database/sql'`, `task lint` and
`task vulncheck` against published dependencies. Source overrides are an explicit development mode;
see the root CONTRIBUTING guide.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
