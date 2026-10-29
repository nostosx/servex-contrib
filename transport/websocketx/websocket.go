// Package websocketx implements the servex WebSocket contracts with
// github.com/coder/websocket. Connections are owned by the accepting handler
// or dialing caller, independently of the HTTP server lifecycle.
package websocketx

import (
	"context"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/coder/websocket"

	"github.com/nostosx/servex/errorx"
	ws "github.com/nostosx/servex/transport/websocket"
)

var (
	_ ws.Acceptor   = (*Acceptor)(nil)
	_ ws.Dialer     = (*Dialer)(nil)
	_ ws.Conn       = (*conn)(nil)
	_ ws.CloseError = (*closeError)(nil)
)

// DefaultReadLimit bounds each received message to 1 MiB.
const DefaultReadLimit = 1 << 20

// Acceptor configures HTTP upgrades. Configure it before use and do not mutate
// Options concurrently. The zero value keeps the SDK's same-origin policy and
// disables compression. Positive ReadLimit overrides DefaultReadLimit.
type Acceptor struct {
	Options   websocket.AcceptOptions
	ReadLimit int64
}

func (a *Acceptor) Accept(w http.ResponseWriter, r *http.Request) (ws.Conn, error) {
	c, err := websocket.Accept(w, r, &a.Options)
	if err != nil {
		return nil, fmt.Errorf("websocketx: accept: %w", err)
	}
	return wrap(c, a.ReadLimit), nil
}

// Dialer configures outgoing handshakes. Configure Options before use. Header
// arguments override matching Options.HTTPHeader values. The caller retains
// ownership of Options.HTTPClient. Positive ReadLimit overrides the default.
type Dialer struct {
	Options   websocket.DialOptions
	ReadLimit int64
}

func (d *Dialer) Dial(ctx context.Context, url string, header http.Header) (ws.Conn, error) {
	opts := d.Options
	opts.HTTPHeader = d.Options.HTTPHeader.Clone()
	if opts.HTTPHeader == nil {
		opts.HTTPHeader = make(http.Header)
	}
	for key, values := range header {
		opts.HTTPHeader.Del(key)
		for _, value := range values {
			opts.HTTPHeader.Add(key, value)
		}
	}
	c, response, err := websocket.Dial(ctx, url, &opts)
	// The SDK transfers the upgraded body to Conn and leaves response.Body nil.
	// Failed handshakes may return a diagnostic body, which we do not retain.
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("websocketx: dial: %w", err)
	}
	return wrap(c, d.ReadLimit), nil
}

type conn struct{ sdk *websocket.Conn }

func wrap(c *websocket.Conn, limit int64) *conn {
	if limit <= 0 {
		limit = DefaultReadLimit
	}
	c.SetReadLimit(limit)
	return &conn{sdk: c}
}

func (c *conn) Read(ctx context.Context) (ws.MessageType, []byte, error) {
	t, data, err := c.sdk.Read(ctx)
	return ws.MessageType(t), data, wrapError(err)
}

func (c *conn) Write(ctx context.Context, t ws.MessageType, data []byte) error {
	if t != ws.Text && t != ws.Binary {
		return errorx.New(errorx.InvalidArgument, "WEBSOCKET_MESSAGE_TYPE", "expected text or binary message")
	}
	if t == ws.Text && !utf8.Valid(data) {
		return errorx.New(errorx.InvalidArgument, "WEBSOCKET_UTF8", "text message must be UTF-8")
	}
	return wrapError(c.sdk.Write(ctx, websocket.MessageType(t), data))
}

func (c *conn) Ping(ctx context.Context) error { return wrapError(c.sdk.Ping(ctx)) }
func (c *conn) Subprotocol() string            { return c.sdk.Subprotocol() }
func (c *conn) Close(code ws.CloseCode, reason string) error {
	return wrapError(c.sdk.Close(websocket.StatusCode(code), reason))
}
func (c *conn) CloseNow() error { return wrapError(c.sdk.CloseNow()) }

type closeError struct {
	cause error
	code  ws.CloseCode
}

func (e *closeError) Error() string             { return e.cause.Error() }
func (e *closeError) Unwrap() error             { return e.cause }
func (e *closeError) CloseStatus() ws.CloseCode { return e.code }

func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if code := websocket.CloseStatus(err); code != -1 {
		return &closeError{cause: err, code: ws.CloseCode(code)}
	}
	return err
}
