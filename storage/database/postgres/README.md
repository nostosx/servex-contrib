# storage/database/postgres

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../../CONTRIBUTING.md#checks).

Shared, driver-neutral SQL helpers and explicit extension administration.
Construction borrows *sql.DB without I/O or ownership. Existing database/sql
and storage/database/gorm adapters still own connection instrumentation and lifecycle.

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

## Capability ownership

| Extension | Integration | Scope |
|---|---|---|
| pgvector | [storage/vector/pgvector](../../vector/pgvector) | Generic vector store, cosine/L2, explicit exact/ANN queries |
| pgvectorscale | storage/vector/pgvector DiskANN index | Requires vectorscale, existing vector data model |
| VectorChord | storage/vector/pgvector VectorChord index | Requires vchord, existing vector data model |
| pg_search | storage/search/postgres ParadeDB | BM25 indexing and conjunction search |
| PGroonga | storage/search/postgres PGroonga | Multilingual Groonga text queries |
| pg_trgm | storage/search/postgres Trigram | GiST similarity retrieval |
| PGMQ | [job/pgmq](../../../job/pgmq) | EnqueueTx, uniqueness, worker retries, leases and archive |
| pg_cron | [scheduler/pgcron](../../../scheduler/pgcron) | Persistent named SQL jobs, explicit removal |
| TimescaleDB | storage/database/postgres CreateHypertable | Time-series storage/continuous aggregates |
| Citus | storage/database/postgres DistributeTable | Requires an explicitly provisioned cluster |
| pg_jsonschema | storage/database/postgres ValidateJSON | Bound JSON Schema/document arguments |
| PostGIS | Application SQL | Spatial types, indexes and queries |
| Apache AGE | Application SQL on a pinned connection | Graph/Cypher, session search_path/LOAD |
| pg_duckdb | Application SQL on a pinned connection | Analytical execution and data-lake access |
| pg_partman | Application migrations | Declarative partition maintenance |
| pg_ivm | Application migrations | Incremental views, supported query subset |
| postgres_fdw | Deployment administration | External database remains independent |
| pg_net | Application SQL | Async HTTP, application retry/reconciliation |
| pg_stat_statements | Diagnostics SQL | Statement-level aggregate statistics |
| pgAudit | Server/session configuration | Audit logging |
| HypoPG | Application SQL on a pinned connection | Hypothetical planning, not real indexes |
| pg_repack | Extension + CLI | Explicit table maintenance |

JSONB, transactions, LISTEN/NOTIFY and logical decoding are native PostgreSQL
features. NOTIFY wakes consumers; durable state belongs in tables or queues.
Extension packages, preload settings and clusters are provisioned explicitly.
Use a deployment role for installation and a restricted runtime role for business
SQL. Keep search_path trusted, bind values, validate identifiers and use versioned
[Goose migrations](../goose) for persistent schema changes.

Recipes and executable coverage live with the tests:
[extension operations](capabilities_integration_test.go),
[data features](data_capabilities_integration_test.go) and
[service features](service_capabilities_integration_test.go).
AGE, HypoPG and session settings require a pinned *sql.Conn.
pg_duckdb aggregate scans use a separate pgx simple-protocol pool because of
its result-type compatibility limitation; see the
[upstream issue](https://github.com/duckdb/pg_duckdb/pull/1051).
Citus needs a provisioned coordinator/worker and a suitable distribution key.
pg_ivm accepts a supported SQL subset, and FDW accesses a separate database.
pg_net HTTP effects need application reconciliation; it is not a durable outbox.
pg_repack needs its matching CLI, disk/lock budgets and an explicit maintenance
operation. Review extension licenses and server-version compatibility separately.

## Integration fixtures

From the repository root, [tests/postgres/fixture.sh](../../../tests/postgres/fixture.sh)
provisions labeled, disposable PostgreSQL 17 containers and installs extensions
inside them; it does not build images. Requires Bash, Docker, curl, Go and
network access to package repositories. SERVEX_POSTGRES_IMAGE selects a
compatible image/digest. Install scripts pin downloaded artifacts and validate
package repositories; record actual installed versions for each run.

| Fixture profile | Coverage | Selected modules |
|---|---|---|
| base | Vector, queue, scheduler, text search and basic operations | All five PostgreSQL capability modules |
| general | DiskANN, VectorChord, JSON Schema, AGE, IVM, HTTP, maintenance, spatial, audit, statistics and FDW | storage/database/postgres, storage/vector/pgvector |
| timescale | Time chunks, continuous aggregates, columnstore and retention | storage/database/postgres |
| citus | Coordinator/worker shards and distributed transaction rollback | storage/database/postgres |
| duckdb | Analytical plans, local Parquet round trips and external-data joins | storage/database/postgres |

```bash
set -e
for profile in base general timescale citus duckdb; do (
  temporary="$(mktemp -d)"
  trap 'if [ -f "$temporary/fixture.env" ]; then bash tests/postgres/fixture.sh down "$temporary/fixture.env"; fi; rm -rf -- "$temporary"' EXIT
  bash tests/postgres/fixture.sh up "$profile" "$temporary/fixture.env"
  set -a
  source "$temporary/fixture.env"
  set +a
  modules=storage/database/postgres
  case "$profile" in
    base) modules='storage/database/postgres storage/vector/pgvector storage/search/postgres job/pgmq scheduler/pgcron' ;;
    general) modules='storage/database/postgres storage/vector/pgvector' ;;
  esac
  task test INTEGRATION=true MODULES="$modules"
); done
```

Fixtures own private networks, random loopback ports and labeled containers;
cleanup removes only matching ownership labels and can be repeated. Generated
environment files contain fixture credentials; keep them private. General uses
a separate FDW peer and HTTP target; Citus uses an actual worker. Never point
these suites at shared services.

SERVEX_IT_POSTGRES_CAPABILITIES selects extended behavior; unknown or duplicate
names fail. SERVEX_IT_VECTOR_INDEX_KINDS selects diskann/vchordrq cases. Index
tests require EXPLAIN evidence before validating results, tenant isolation and
transaction behavior; creating an index alone is insufficient. The gate rejects
skipped/absent tests. These checks do not establish production performance or HA.
Source/standalone checks follow [CONTRIBUTING](../../../CONTRIBUTING.md#checks).
