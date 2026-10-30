# transport/httpx

HTTP transport.

Implements core transport.Server and HTTP clients with net/http. Routing,
request binding, body limits, error mapping, health endpoints and native HTTP
middleware moved intact from core. JSON and random defaults are selected
explicitly by this contrib adapter; the shared core client selects neither.

## Source migration

Browser-facing applications can compose the optional transport/cors adapter and
Go's standard-library http.CrossOriginProtection through WithHTTPMiddleware.
CORS belongs outside authentication/routing so preflight requests terminate
before credentials are required. CrossOriginProtection rejects unsafe cross-site
browser requests and needs explicit trusted origins when a separate application
origin is permitted. It is independent of CORS response access policy. Keep
GET/HEAD/OPTIONS free of state changes. A complete HTTPS example lives in
[transport/cors](../cors/README.md); csrf_test.go exercises the standard-library
composition without adding a CSRF SDK to this module.

Requires servex v1.4.0. Change
imports from the previous core location to
`github.com/nostosx/servex-contrib/transport/httpx`. Constructors/options
otherwise preserve the moved implementation's API. Core contracts remain
under `github.com/nostosx/servex`.

## Validation

Run repository `task check MODULES='transport/httpx'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/transport/httpx@v1.0.0
```
