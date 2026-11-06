# storage/cache/memory

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../../CONTRIBUTING.md#checks).

In-memory cache.

Implements the core cache.Cache contract using kitx/ttlcache. Stored and
returned bytes are copied; expiration, capacity, clock injection and LRU
semantics are preserved. The original cache regression suite lives here.

## Validation

Run repository `task check MODULES='storage/cache/memory'`, `task lint` and
`task vulncheck` against published dependencies. Source overrides are an explicit development mode;
see the root CONTRIBUTING guide.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
