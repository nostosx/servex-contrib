# transport/websocketx

Requires servex v1.4.0 with its published protocol contracts.

Implements the core `websocket.Acceptor`, `websocket.Dialer` and `websocket.Conn`
with `github.com/coder/websocket`. Applications use core interfaces for message
I/O, Ping, subprotocols and close operations; SDK-specific handshake options
remain in this module. Peer close errors implement core `websocket.CloseError`
and retain the original SDK error through Unwrap.

## Server

```go
package main

import (
	"log"
	"net/http"

	"github.com/nostosx/servex-contrib/transport/websocketx"
)

func main() {
	acceptor := new(websocketx.Acceptor)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := acceptor.Accept(w, r)
		if err != nil {
			return // Accept has already written the handshake failure.
		}
		defer conn.CloseNow()
		for {
			typ, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", mux))
}
```

Mount the same handler through contrib `httpx.Server.HandleHTTP` to use its
HTTP middleware and lifecycle. Authenticate before upgrading. The zero-value
acceptor retains the SDK's same-origin policy; configure specific
`Options.OriginPatterns` for cross-origin browser access. Compression is off
by default. Subprotocol negotiation uses `Options.Subprotocols` on both sides.

## Client and ownership

`(&websocketx.Dialer{}).Dial(ctx, url, headers)` returns a core connection.
Handshake headers override matching `Options.HTTPHeader` values. The optional
SDK HTTP client remains caller-owned. Configure option structs before sharing
an acceptor or dialer; do not mutate them concurrently with calls.

Each message is bounded by `DefaultReadLimit` (1 MiB); a positive ReadLimit
overrides it. Text writes must be UTF-8. One reader may run concurrently with
writes and Ping. Keep a reader active to process control frames; Ping waits for
that reader to observe the pong. Individual Read calls must be serialized.

The dial context bounds the handshake only. I/O contexts bound individual
operations; cancellation may close a connection, but a pre-canceled operation
may return without touching it. The caller must always release the connection.
Use Close for a bounded protocol handshake and defer CloseNow as final cleanup.
Messages and streams are never replayed automatically.

HTTP Shutdown does not wait for or close upgraded connections. Applications
must track active connections, close them on shutdown and join their handlers.
Do not retain r.Context for background use after the HTTP handler returns.

[Browser acceptance](../../tests/browser) uses native WebSocket in Chrome and
WebKit to verify text/binary messages, subprotocol negotiation, a clean close
handshake and Origin rejection. Cross-origin allowlist patterns include the
origin host and port. WebSocket Origin checking is independent of HTTP CORS.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/transport/websocketx@v1.0.0
```
