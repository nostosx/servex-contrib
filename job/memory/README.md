# job/memory

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../CONTRIBUTING.md#checks).

In-memory job queue.

Implements job.Enqueuer, Runner and ReadyRunner with delay, retries,
unique reservations, concurrency limits and cancellation. The workertest
subpackage retains the core Worker lifecycle/readiness regression suite.

## Validation

Run repository `task check MODULES='job/memory'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
