# job/memory

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

In-memory job queue.

Implements job.Enqueuer, Runner and ReadyRunner with delay, retries,
unique reservations, concurrency limits and cancellation. The workertest
subpackage retains the core Worker lifecycle/readiness regression suite.

## Validation

Run repository `task check MODULES='job/memory'`, `task lint` and
`task vulncheck` against published dependencies. Source overrides are an explicit development mode;
see the root CONTRIBUTING guide.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
