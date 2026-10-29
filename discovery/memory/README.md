# discovery/memory

In-memory registry.

Implements discovery.Registrar and discovery.Discoverer with copied snapshots,
versioned change notification and cancellable watchers. The transporttest
subpackage checks core shared-client behavior with this explicitly selected
backend and an explicit random selector.

## Source migration

Requires servex v1.4.0. Change
imports from the previous core location to
`github.com/nostosx/servex-contrib/discovery/memory`. Constructors/options
otherwise preserve the moved implementation's API. Core contracts remain
under `github.com/nostosx/servex`.

## Validation

Run repository `task check MODULES='discovery/memory'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/discovery/memory@v1.0.0
```
