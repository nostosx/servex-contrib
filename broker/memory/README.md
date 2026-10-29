# broker/memory

In-memory broker.

Implements broker.Publisher, Subscriber and ReadySubscriber. Consumer groups,
broadcast delivery, retry/dead-letter policy, cancellation and copied message
data retain the original regression coverage. consumertest checks core
consumer lifecycle composition against this explicit backend.

## Source migration

Requires servex v1.4.0. Change
imports from the previous core location to
`github.com/nostosx/servex-contrib/broker/memory`. Constructors/options
otherwise preserve the moved implementation's API. Core contracts remain
under `github.com/nostosx/servex`.

## Validation

Run repository `task check MODULES='broker/memory'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/broker/memory@v1.0.0
```
