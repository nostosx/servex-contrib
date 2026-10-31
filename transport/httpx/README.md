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

## Restricted outbound HTTP

`NewRestrictedHTTPClient(HTTPPolicy{})` supplies a bounded client for untrusted
URLs, independently of typed Client/Server APIs. Defaults: 30-second timeout,
16 MiB decompressed response body, no environment proxy/retry/cookie jar. It
accepts HTTP(S) URLs without credentials, rejects private/reserved IPs including
IPv4-mapped addresses and translation/tunnel ranges, checks every DNS answer,
and dials only checked IPs while retaining the TLS hostname. Redirects repeat
validation, reject HTTPS downgrade and stop at ten hops. CloseIdleConnections
releases owned transport connections. `ErrResponseTooLarge` detects both known
Content-Length and streaming overflow. `AllowPrivateNetwork` is explicit for
trusted internal endpoints; all other limits still apply.

This shared foundation is used by AgentX web readers/connectors; core AgentX
does not depend on Servex. Applications using a custom client own its network
and proxy policy.

受限出站 HTTP 客户端支持 DNS 地址固定、重定向检查、私网/保留地址拒绝和响应上限；
默认不继承环境代理，不增加自动重试。可信内部服务可显式允许私网访问。
