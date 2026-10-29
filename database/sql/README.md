# database/sql

database/sql adapter.

Package database wraps a caller-selected database/sql driver/Connector for
tracing, metrics and lifecycle/readiness. It returns native *sql.DB values.
There is no new database abstraction or ORM. No-op telemetry defaults live
in contrib; existing optional driver capabilities and error identities are preserved.

## Source migration

This module is unreleased and requires the matching unreleased core. Change
imports from the previous core location to
`github.com/nostosx/servex-contrib/database/sql`. Constructors/options
otherwise preserve the moved implementation's API. Core contracts remain
under `github.com/nostosx/servex`.

## Validation

Run repository `task check MODULES='database/sql'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
