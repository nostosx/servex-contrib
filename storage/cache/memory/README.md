# storage/cache/memory

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

In-memory cache.

Implements the core cache.Cache contract using kitx/ttlcache. Stored and
returned bytes are copied; expiration, capacity, clock injection and LRU
semantics are preserved. The original cache regression suite lives here.

## Validation

Run repository `task check MODULES='storage/cache/memory'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
