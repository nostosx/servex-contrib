# PostgreSQL capabilities

Servex v1.5.0 defines the contracts. The five contrib modules below are
independently published as v1.0.0: database/postgres, vector/pgvector,
search/postgres, jobqueue/pgmq and scheduler/pgcron. Their manifests select real
published dependencies, without source replacements or placeholder versions.

## Ownership and layout

Core owns storage/vector and storage/search contracts. Backend execution,
configuration, SQL, indexes and migrations live in contrib. Apps inject their
SQL pools and trusted tenant namespaces; constructors create no tables, install
no extensions and start no background work. SQL/GORM instrumentation remains in
the existing database/sql and database/gorm modules. Database privileges/RLS,
not caller-supplied namespaces, enforce database access control.

| Extension | Integration | Scope |
|---|---|---|
| pgvector | vector/pgvector | Generic vector store, cosine/L2, explicit exact/ANN queries |
| pgvectorscale | vector/pgvector DiskANN index | Requires vectorscale, existing vector data model |
| VectorChord | vector/pgvector VectorChord index | Requires vchord, existing vector data model |
| pg_search | search/postgres ParadeDB | BM25 indexing and conjunction search |
| PGroonga | search/postgres PGroonga | Multilingual Groonga text queries |
| pg_trgm | search/postgres Trigram | GiST similarity retrieval |
| PGMQ | jobqueue/pgmq | EnqueueTx, uniqueness, worker retries, leases and archive |
| pg_cron | scheduler/pgcron | Persistent named SQL jobs, explicit removal |
| TimescaleDB | database/postgres CreateHypertable + SQL below | Time-series storage/continuous aggregates |
| Citus | database/postgres DistributeTable + SQL below | Requires an explicitly provisioned cluster |
| pg_jsonschema | database/postgres ValidateJSON | Bound JSON Schema/document arguments |
| PostGIS | Native SQL recipe below | Spatial types, indexes and queries |
| Apache AGE | Pinned SQL connection recipe below | Graph/Cypher, session search_path/LOAD |
| pg_duckdb | Pinned SQL connection recipe below | Analytical execution and data-lake access |
| pg_partman | Explicit migration recipe below | Declarative partition maintenance |
| pg_ivm | Explicit migration recipe below | Incremental views, supported query subset |
| postgres_fdw | Explicit administrative recipe below | External database remains independent |
| pg_net | Explicit SQL recipe below | Async HTTP, application retry/reconciliation |
| pg_stat_statements | Diagnostics SQL below | Statement-level aggregate statistics |
| pgAudit | Server/session configuration below | Audit logging |
| HypoPG | Pinned SQL connection recipe below | Hypothetical planning, not real indexes |
| pg_repack | Extension + CLI below | Explicit table maintenance |

JSONB, ordinary transactions, LISTEN/NOTIFY and logical decoding are native
PostgreSQL facilities. A plugin is not required to use them. NOTIFY is a wakeup
signal; durable state remains in a table/queue.

Agentx's existing pgvector backend retains its own schemas and exact-search
semantics. The new agentx-contrib/retrieval NewServexVector/NewServexText
adapters consume injected core contracts and encode rag.Chunk as opaque payload.
They do not replace the existing memory/RAG store or migrate its data. See that
module's README for supported filters and score conversion.

## Standalone acceptance and optional source development

Use Go 1.27.3, Task 3.53.1 and the repository's lint/vulnerability tools.
Normal release acceptance is standalone, using published modules:

```sh
task check lint vulncheck MODULES='database/postgres vector/pgvector search/postgres jobqueue/pgmq scheduler/pgcron'
```

For explicitly requested work across unpublished peer changes, the optional
development helper keeps overrides outside the repository:

```sh
bash .github/scripts/local-peers.sh /absolute/path/to/servex \
  task check lint vulncheck \
  MODULES='database/postgres vector/pgvector search/postgres jobqueue/pgmq scheduler/pgcron'
```

GO can select an absolute Go binary. The helper copies manifests/sums to an
external temporary directory, adds source replacements there, and supplies a
temporary go wrapper to Task/tools. Public checksums for replaced modules are
omitted only from those temporary sums. It never changes checked-in manifests,
creates a go.work, modifies Git state or installs a global Go configuration.
Ordinary standalone tasks remain the release acceptance path after real core
and foundation versions have been published and consumers updated.

Set SERVEX_DEV_CONSUMER to an external consumer module (for example the
servex-examples root) to run the same explicit source validation there. This
adds an external consumer modfile; ordinary Taskfiles remain independent.

### Disposable fixtures without image builds

`tests/postgres/fixture.sh` runs upstream PostgreSQL 17 containers, installs
extension packages inside those containers with `docker exec`, configures preload,
restarts, and waits for the final TCP server. It never builds an image. It needs
Bash, Docker, curl, Go and network access to PGDG, Pigsty, GitHub and (base only) Groonga.
The default image is `postgres:17` with Debian trixie; SERVEX_POSTGRES_IMAGE can
select a compatible pinned image/digest. Upstream release artifacts are pinned
in install.sh; the other packages resolve through signed APT repositories.
A checksum-pinned Debian CA package bootstraps container TLS; APT uses HTTPS
and continues verifying signed indexes and package checksums. Record actual
server/package/SQL extension versions for each acceptance run.

| Profile | Installed capabilities | Test modules |
|---|---|---|
| base | pgvector, PGMQ, pg_cron, ParadeDB, PGroonga, trigram and basic operations extensions | All five new modules |
| general | DiskANN, VectorChord, JSON Schema, AGE, IVM, pg_net, repack, partman, PostGIS, audit, statistics, HypoPG, FDW | database/postgres vector/pgvector |
| timescale | TimescaleDB including continuous aggregates and columnstore | database/postgres |
| citus | Separate coordinator and worker, actual distributed shards | database/postgres |
| duckdb | pg_duckdb analytical plan and local Parquet round trip | database/postgres |

Each profile owns a unique labeled network, loopback host ports and containers.
General includes a separate FDW peer and a container-local HTTP target. Citus
includes a real worker. Test-only trust authentication is confined to that
fixture's private network and loopback host ports. The generated environment
file includes public fixture credentials; keep the file private to the run.
`down` removes only resources carrying its matching ownership label and is safe
to repeat. An installation failure also cleans owned resources.

Run every profile from the repository root (Bash):

```bash
set -e
for profile in base general timescale citus duckdb; do (
  temporary="$(mktemp -d)"
  trap 'if [ -f "$temporary/fixture.env" ]; then bash tests/postgres/fixture.sh down "$temporary/fixture.env"; fi; rm -rf -- "$temporary"' EXIT
  bash tests/postgres/fixture.sh up "$profile" "$temporary/fixture.env"
  set -a
  source "$temporary/fixture.env"
  set +a
  modules=database/postgres
  case "$profile" in
    base) modules='database/postgres vector/pgvector search/postgres jobqueue/pgmq scheduler/pgcron' ;;
    general) modules='database/postgres vector/pgvector' ;;
  esac
  bash .github/scripts/local-peers.sh /absolute/path/to/servex \
    task test INTEGRATION=true MODULES="$modules"
done
```

The base CI integration row uses the same script. The separate
postgres-capabilities CI matrix covers the other four profiles without
repeating the primary module-coverage matrix; ordinary CI resolves the published versions directly. Local-source development
uses the explicit helper and does not establish public consumer acceptance.

Capability selection is explicit through SERVEX_IT_POSTGRES_CAPABILITIES;
unknown/repeated names fail. SERVEX_IT_VECTOR_INDEX_KINDS selects diskann and
vchordrq tests. Each index/metric has its own 512-record table and must appear in
EXPLAIN before exact/ANN result, metadata, tenant isolation, update, delete and
rollback checks pass. A successful CREATE INDEX alone does not pass acceptance.

The general database suite verifies JSON CHECK rejection, transactional IVM,
parameterized Cypher/graph rollback, partition expiration, geography/GiST plans,
HypoPG planning and session isolation, actual statement counts and audit logs,
physical table replacement under concurrent writes, asynchronous HTTP commit/
rollback/503, and remote FDW read/write/rollback/predicate pushdown. The dedicated
profiles verify time chunks, aggregate refresh, columnstore and retention;
coordinator/worker shards and distributed rollback; DuckDB plans, Parquet output/
input and joins between PostgreSQL and external data. The gate uses the race
detector and rejects skipped/absent tests. These functional tests do not measure
production-scale performance, HA or certify all extensions in one server.

## SQL and operations recipes

All SQL below is explicit application/deployment configuration. Use a privileged
deployment role for extension installation and a restricted runtime role for
business operations. Keep search_path restricted to trusted schemas. Values
must be bound parameters; identifiers must pass postgres.Ident/Table validation.
Use versioned migrations (existing database/goose) for persistent schema changes.

### TimescaleDB

Install/preload the compatible server package, enable timescaledb, and create
an ordinary timestamped table before Client.CreateHypertable. For example:

```sql
CREATE TABLE reporting.events (occurred_at timestamptz NOT NULL, tenant text NOT NULL, value double precision NOT NULL);
SELECT create_hypertable('reporting.events'::regclass, 'occurred_at'::name, if_not_exists => true);
CREATE MATERIALIZED VIEW reporting.hourly WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour',occurred_at) AS bucket, tenant, sum(value) AS value
  FROM reporting.events GROUP BY bucket,tenant;
SELECT add_continuous_aggregate_policy('reporting.hourly',
  start_offset => interval '2 days', end_offset => interval '1 hour',
  schedule_interval => interval '1 hour');
```

### PostGIS

```sql
CREATE TABLE reporting.places (id bigint PRIMARY KEY, position geography(Point,4326) NOT NULL);
CREATE INDEX places_position ON reporting.places USING gist(position);
-- Bound arguments: longitude, latitude, radius in meters.
SELECT id FROM reporting.places
WHERE ST_DWithin(position,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,$3);
```

### Apache AGE

Use a pinned *sql.Conn for session setup and all Cypher calls; a pooled Exec
followed by pooled Query does not guarantee the same session. Configure AGE's
required LOAD and search_path for the selected server version, for example:

```sql
LOAD 'age';
SET search_path = ag_catalog, public;
SELECT create_graph('knowledge');
SELECT * FROM cypher('knowledge', $$ MATCH (n) RETURN n LIMIT 10 $$) AS (node agtype);
```

Graph names and Cypher programs are trusted configuration. Parameterize query
values using AGE's version-specific parameter API rather than interpolating them.

### pg_duckdb

Use a pinned *sql.Conn for settings. pg_duckdb has its own supported SQL/type and
transaction boundaries. With the accepted 1.1.1 package, extended-protocol
Describe can report an aggregate type that differs from execution, causing pgx
to fail with `invalid length for int8`. Use a dedicated pgx simple-protocol pool
for analytical queries, as the real integration test does; application values
remain bound arguments through pgx. The ordinary OLTP pool keeps its own policy.
See the [upstream result-type issue](https://github.com/duckdb/pg_duckdb/pull/1051).

```go
cfg, err := pgx.ParseConfig(dsn)
if err != nil { return err }
cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
analytics := stdlib.OpenDB(*cfg) // github.com/jackc/pgx/v5/stdlib
// The application owns analytics.Close and pins a connection for session settings.
```

```sql
SET duckdb.force_execution = true;
SELECT tenant, sum(value) FROM reporting.events GROUP BY tenant;
-- Object-store credentials are injected separately; never commit them in SQL.
SELECT count(*) FROM read_parquet('s3://application-data/events/*.parquet');
```

### Citus

Provision coordinator/worker nodes and placement before sharding. Choose a
distribution key that supports co-located business transactions:

```sql
SELECT create_distributed_table('reporting.events'::regclass,'tenant');
```

Citus hides worker shard tables from ordinary catalog queries. Diagnostic-only
inspection can set `citus.override_table_visibility=off` on a pinned worker
connection; the acceptance test then checks actual worker tables.

Client.DistributeTable binds the relation/key arguments. Validate unique/foreign
key restrictions and cross-shard queries against the target Citus version.

### pg_partman and pg_ivm

```sql
-- pg_partman installed in schema partman; the parent must already be partitioned.
SELECT partman.create_parent(p_parent_table := 'reporting.events',
  p_control := 'occurred_at', p_interval := '1 day', p_premake := 3);
-- Run maintenance through its background worker or an explicit pg_cron job.
CALL partman.run_maintenance_proc();

-- pg_ivm: accepted queries are a supported subset, not arbitrary SQL views.
SELECT pgivm.create_immv('reporting.summary',
  'SELECT tenant,count(*) FROM reporting.events GROUP BY tenant');
```

### postgres_fdw and pg_jsonschema

```sql
CREATE SERVER analytics FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'analytics.internal', dbname 'analytics', port '5432');
-- Provision a runtime-role user mapping separately with managed credentials.
IMPORT FOREIGN SCHEMA reporting FROM SERVER analytics INTO external_reporting;

SELECT jsonb_matches_schema('{"type":"object","required":["name"]}'::json,
  '{"name":"servex"}'::jsonb);
```

FDW is remote access, not a copy of the external database. Client.ValidateJSON
provides the bound-parameter form of the validation call.

### pg_net and pg_cron

```sql
SELECT net.http_post(url := 'https://application.internal/webhooks/refresh',
  body := '{"kind":"refresh"}'::jsonb);
```

For recurring servex work, call queue.ScheduleCommand with a job template,
then pass its returned command to pgcron.Schedule. Each execution generates a
fresh UUID and atomically reserves/sends the proper servex envelope. Fixed IDs
and uniqueness options are deliberately rejected for recurring templates.
pg_net's async HTTP request storage/retry semantics are separate from the durable
business outbox; use explicit reconciliation for uncertain remote effects.

### Diagnostics and maintenance

```sql
SELECT queryid,calls,total_exec_time,rows FROM pg_stat_statements
ORDER BY total_exec_time DESC LIMIT 20;
-- pgAudit must be preloaded; choose logging policy during deployment.
SET pgaudit.log = 'write,ddl';
-- HypoPG state is connection-local: use one pinned *sql.Conn.
SELECT * FROM hypopg_create_index('CREATE INDEX ON reporting.events(tenant)');
EXPLAIN SELECT * FROM reporting.events WHERE tenant='example';
SELECT hypopg_reset();
```

pg_repack requires its matching client binary as well as the server extension:

```sh
pg_repack --dbname="$DATABASE_URL" --table=reporting.events --no-order
```

Run it as an explicit operational action with adequate disk space and lock
budgets; no application startup path invokes repack. Review each extension's
license and compatibility for your distribution (not all server extensions use
the adapter's MIT license).
