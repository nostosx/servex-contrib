# discovery/memory

Module version for this revision: **v1.0.1**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

In-memory registry.

Implements discovery.Registrar and discovery.Discoverer with copied snapshots,
versioned change notification and cancellable watchers. The transporttest
subpackage checks core shared-client behavior with this explicitly selected
backend and an explicit random selector.

## Validation

Run repository `task check MODULES='discovery/memory'`, `task lint` and
`task vulncheck` against published dependencies. Source overrides are an explicit development mode;
see the root CONTRIBUTING guide.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
