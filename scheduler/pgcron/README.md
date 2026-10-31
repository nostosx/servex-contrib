# pg_cron schedule management (v1.0.0)

Explicit SQL schedule management using a caller-owned *sql.DB. No constructor
I/O, extension installation, Go callback execution, or pool ownership.

```go
scheduler, err := pgcron.New(db, "myapp")
if err != nil { return err }
if err := scheduler.Check(ctx); err != nil { return err }
id, err := scheduler.Schedule(ctx, "refresh", "*/5 * * * *",
    "REFRESH MATERIALIZED VIEW reporting.summary")
jobs, err := scheduler.Jobs(ctx)
removed, err := scheduler.Unschedule(ctx, "refresh")
```

Schedule creates or updates the current user's named job. A trusted prefix
isolates this manager's names; Jobs and Unschedule also constrain the current
database user. Prefixes are not authorization. pg_cron privileges/RLS remain
the security boundary. Unscheduling an absent job returns false.

Command is trusted administrative SQL, never request/model input. Schedules
and timezone follow the server's pg_cron configuration. Jobs persist when
the Go process stops; there is intentionally no lifecycle Stop that removes
them. Unschedule explicitly when the application owns their deletion.

Use SQL to enqueue work into PGMQ when the actual handler lives in Go.
Install/preload pg_cron, configure cron.database_name and connection/worker
execution before enabling it. The acceptance suite checks named update,
persistence, prefix isolation and idempotent removal. It does not claim Go
callbacks run inside PostgreSQL or exactly-once business effects.

See [PostgreSQL integration](../../POSTGRESQL.md) for unreleased dependency
setup and the disposable real-server fixture.

Qualified prefix:name is limited to 63 bytes; prefix leaves at least one byte for a name. Oversized names fail before SQL, avoiding PostgreSQL name truncation.
