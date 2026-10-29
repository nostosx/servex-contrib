# Browser protocol acceptance

Runs real Google Chrome and Playwright WebKit against the matching local core
and contrib sources. It uses native EventSource/WebSocket and the official
Connect-Web 2.3.0 client, rather than a DOM mock or Go client simulation.
The disposable Go fixture uses the actual contrib HTTP server, SSE, WebSocket
and Connect adapters with the Go race detector enabled.

## Run

Requires Go 1.27.3, Node.js, npm, protoc, and installed Google Chrome. From this
directory:

```sh
npm ci --ignore-scripts
npx playwright install webkit
npm test
```

`GO` and `PROTOC` select executables. `SERVEX_CORE` selects the matching core
checkout; by default it is the sibling `servex` directory. `BROWSERS=chrome`
or `BROWSERS=webkit` selects one engine; the default runs both. `BROWSER_OUTPUT`
selects the output directory, defaulting to the ignored `results/` directory.

The runner creates an external temporary Go module with explicit source
replacements for the local repositories. It never edits library go.mod/go.sum
files or commits a workspace. Generated JavaScript descriptors stay under
ignored `.generated/`; the server executable and bundle stay in a temporary
directory. Servers listen only on random loopback ports and stop after testing.
No shared services, credentials, external API requests or publication are needed.

## Coverage

There are 48 checks per engine, covering same-origin access, explicitly allowed
cross-origin access, and rejected origins:

- Native SSE receives a UTF-8 multiline event before the handler finishes,
  preserves custom event types/IDs, resets an empty ID, reconnects using native
  Last-Event-ID, and closes the server request when EventSource.close runs.
- Native WebSocket negotiates a subprotocol, echoes UTF-8 text and binary data,
  completes a clean close handshake, and rejects an unapproved Origin.
- Connect uses both Protobuf and JSON; gRPC-Web uses Protobuf. Both run unary
  calls and server streams, preserve application headers/trailers and structured
  error details, and report errors after a partial response.
- AbortSignal and early return from the async iterator both cancel the actual
  server stream. A control endpoint holds the handler open until the browser
  acknowledges its first message, proving responses were not buffered until EOF.
- Official browser transports reject client-streaming and bidirectional requests.
  These are expected platform limitations and are not counted as supported RPC
  shapes. Only unary and server streaming are established for browsers.

The fixture owns an explicit CORS allowlist, preflight handling and exposed
headers. Connect unary trailers require exposing their Trailer-* response
headers. WebSocket Origin checking is independent of CORS. The adapters do not
automatically grant browser cross-origin access.

The browser path uses loopback HTTP/1.1. Existing Go transport tests separately
cover HTTP/2 and h2c; this suite does not claim browser TLS, reverse-proxy or
native Safari app acceptance. WebKit is the Playwright engine build.

Each engine writes a JSON report (version, user agent, every assertion and browser
console output), a screenshot and fixture diagnostics. `report.json` combines
the selected engines. Expected CORS diagnostics from the denied-origin page are
retained; unexpected page errors, failed checks and Go races fail the command.
