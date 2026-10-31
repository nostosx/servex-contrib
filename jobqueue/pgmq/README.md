# PGMQ worker adapter (v1.0.0)

Implements `jobqueue.Enqueuer` and `jobqueue.ReadyRunner` using PGMQ >= 1.5.
The caller owns the SQL pool. Construction performs no I/O. Install PGMQ on the
server and enable it in the database, then explicitly call Migrate once.

```go
queue, err := pgmq.New(db, pgmq.Config{Queue: "jobs", Concurrency: 4})
if err != nil { return err }
if err := queue.Migrate(ctx); err != nil { return err }
id, err := queue.Enqueue(ctx, &jobqueue.Job{Type: "index", Payload: payload},
    jobqueue.WithMaxRetry(3), jobqueue.WithUnique("source-1", time.Minute))
```

For database-driven recurring work, build SQL with
`command, err := queue.ScheduleCommand(&jobqueue.Job{Type: "index", Payload: payload})`
and pass command to pgcron.Schedule. Every execution atomically reserves a fresh
UUID and sends a valid envelope. Fixed IDs and UniqueKey are rejected for this
recurring template API. No arbitrary payload is interpolated as SQL.

Register `jobqueue.NewWorker(queue, handlers, ...)` with your existing App, or
call Run/RunReady. A nil handler result deletes the message; retryable errors
reschedule with exponential 1..64 second backoff, up to MaxRetry; permanent
errors, panics, missing handlers and exhausted deliveries archive it. Attempt is
the PGMQ delivery count, so crashes consume attempts. Malformed messages are
archived. Inspect/replay archives explicitly with PGMQ tooling.
Archived records include `_servex_failure` with delivery attempt, server timestamp
and failure classification; private handler messages/panic values are not copied
into that diagnostic field. Length counts queued/in-flight messages, excluding archives.

EnqueueTx receives a caller-owned *sql.Tx. Business writes and enqueue commit or
roll back together. The caller MUST roll back on any EnqueueTx error, including
ErrDuplicate. This adapter never commits that transaction. A lost commit response
is an uncertain outcome: reconcile the business idempotency key before retrying.

UniqueKey with positive TTL stays reserved for that TTL even after completion.
With nonpositive TTL it stays reserved until terminal settlement. IDs are reserved
while queued/in flight. A duplicate fails before sending. Reservation updates and
send share the transaction. PruneExpired removes a bounded batch of elapsed
positive-TTL reservations. Operators deleting messages outside this adapter must
reconcile lifetime reservations; constructors never delete them automatically.
Positive TTL uses PostgreSQL timestamp precision and starts at the reservation
statement; transaction lifetime matters for EnqueueTx. PGMQ delay/visibility
use whole seconds, with a positive fractional enqueue delay rounded upward.

Visibility defaults to 30s (whole seconds, 3s..24h). Heartbeats run every third of
the visibility period. Renewal and settlement verify the delivery read_ct and
unexpired visibility, so an old worker cannot renew or acknowledge a new receipt.
This requires PGMQ's pgmq.q_<queue> row layout (msg_id/read_ct/vt); validate
extension upgrades with the real-server suite. External handlers can still race
after lease loss and MUST enforce business idempotency. This is at-least-once
processing, not an exactly-once side-effect guarantee.

Concurrency is bounded (1..128), polling defaults to 100ms, payloads are bounded.
Run cancellation leaves unfinished messages for redelivery. Close(ctx) cancels
active operations and waits within ctx without closing db. Uncooperative handlers
can outlive the deadline; later Close can continue waiting. Ready is reported
only after both owned tables can be queried.
Temporary database/network/serialization errors back off (100ms minimum,
30s maximum) and recover within the running worker. Permanent schema/permission
errors stop it. Producer writes are never automatically retried after an
uncertain commit; that decision remains with the business transaction owner.

See [PostgreSQL integration](../../POSTGRESQL.md). This module does not implement
broker broadcast/group/dead-letter contracts; a durable task queue alone does
not establish those semantics.

Renewal and settlement acquire the message row lock before checking server time. A receipt that expires while waiting is rejected, even if the lock holder never updates the row. Consumer envelopes enforce the same type/ID/payload bounds as enqueue; malformed or oversized messages are archived without handler execution.
