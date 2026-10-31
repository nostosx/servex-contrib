# PostgreSQL foundations (v1.0.0)

Shared, driver-neutral SQL helpers and explicit extension administration.
Construction borrows *sql.DB without I/O or ownership. Existing database/sql
and database/gorm adapters still own connection instrumentation and lifecycle.

```go
client, err := postgres.New(db)
if err != nil { return err }
available, err := client.Extensions(ctx) // Availability and installed version/schema.
if err := postgres.Require(ctx, db, "vector", "pgmq"); err != nil { return err }
// Administrative opt-in only, after server packages/preload have been configured:
if err := client.Enable(ctx, "pg_trgm"); err != nil { return err }
```

Enable never downloads packages, sets preload libraries, restarts PostgreSQL,
or uses CASCADE. Require reports a missing extension as FailedPrecondition.
Check implements servex health checking. Ident validates and quotes independent
lowercase identifiers (up to 63 bytes); values always use query parameters.
Labels preserves exact UTF-8 JSONB strings instead of silently replacing invalid
UTF-8. DBTX is satisfied by *sql.DB and *sql.Tx.

Transaction runs once, rolls back on errors/panics, preserves errors.Is/As and
joins a rollback failure with the original error. Retrying business transactions
is the application's policy. SQLSTATE classification preserves driver errors;
unknown errors are not blindly marked retryable.

CreateHypertable, DistributeTable and ValidateJSON provide small explicit
administrative operations for TimescaleDB, Citus and pg_jsonschema. Their SQL
functions must be installed and resolvable on a trusted search_path. They are
not separate ORMs or replacements for native SQL.

[PostgreSQL integration](../../POSTGRESQL.md) maps every discussed extension to
a runtime adapter, management helper or executable SQL/operations recipe.

The real integration suite selects extended behaviors with
SERVEX_IT_POSTGRES_CAPABILITIES. The general/timescale/citus/duckdb fixture
profiles exercise those behaviors on isolated servers, including a separate FDW
peer, Citus worker and HTTP target. See the integration guide for exact coverage
and the pg_duckdb simple-protocol compatibility requirement.
