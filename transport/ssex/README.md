# transport/ssex

Unreleased. This module requires the matching servex sources containing
`transport/sse`; the declared core version is a release preparation baseline,
not a claim that the new contracts are already published.

Implements `sse.Acceptor`, `sse.Sender`, `sse.Client` and `sse.Receiver` using
`net/http`, without an additional SDK. Each send flushes immediately. Events
support IDs, event types, multiline UTF-8 data and millisecond retry hints;
`Comment` sends heartbeat comments. Empty IDs explicitly reset Last-Event-ID.

## Server

```go
package main

import (
	"log"
	"net/http"

	"github.com/nostosx/servex/transport/sse"
	"github.com/nostosx/servex-contrib/transport/ssex"
)

func main() {
	acceptor := new(ssex.Acceptor)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		sender, err := acceptor.Accept(w, r)
		if err != nil {
			log.Print(err)
			return
		}
		if err := sender.Send(sse.Event{ID: "1", Type: "greeting", Data: "hello\nworld"}); err != nil {
			log.Print(err)
		}
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", mux))
}
```

The same handler mounts through contrib `httpx.Server.HandleHTTP`, inheriting
HTTP middleware. It does not inherit endpoint middleware. Keep the handler
alive while producing events and stop on `r.Context().Done()`. Serialize sender
calls and finish them before returning. After a write or flush failure, return
without appending a JSON error or retrying a partially sent event. Validation
errors from Accept occur before commitment; initial flush errors may occur after it.

For long streams, configure response write deadlines and proxy buffering for
streaming. Heartbeat scheduling and graceful stream cancellation are application
responsibilities; this adapter starts no background goroutines.

## Client

`(&ssex.Client{HTTP: client, MaxEventBytes: 1 << 20}).Open(ctx, url, headers)`
returns a core `sse.Receiver`. HTTP and its transport remain caller-owned.
Always close the receiver; Close interrupts a blocked Receive. Receive returns
`io.EOF` on normal completion and retains the terminal error on subsequent calls.
Canceling ctx terminates the request. Do not call Receive concurrently.

The parser accepts BOM, LF/CRLF/CR, multiline data and persistent IDs/retry hints.
Comments and unknown fields are ignored; incomplete events at EOF are discarded.
The default 1 MiB limit bounds an entire event block, including comments, and
individual lines. Oversized input returns `errorx.ResourceExhausted`.
Non-200 responses and non-SSE media types are rejected and closed.

There is no automatic reconnect or event store. Read `Last-Event-ID` from the
request when implementing replay. Clients supply it in the next Open call's
headers and choose whether an operation is safe to reconnect. Retry is a hint,
not a command to replay requests automatically.
