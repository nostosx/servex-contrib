# discovery/memory

In-memory registry.

Implements discovery.Registrar and discovery.Discoverer with copied snapshots,
versioned change notification and cancellable watchers. The transporttest
subpackage checks core shared-client behavior with this explicitly selected
backend and an explicit random selector.

## Source migration

This module is unreleased and requires the matching unreleased core. Change
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
