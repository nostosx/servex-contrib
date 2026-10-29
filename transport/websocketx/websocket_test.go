package websocketx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nostosx/servex-contrib/transport/websocketx"
	"github.com/nostosx/servex/errorx"
	ws "github.com/nostosx/servex/transport/websocket"
)

func TestMessagesAndHandshake(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a := &websocketx.Acceptor{Options: websocket.AcceptOptions{Subprotocols: []string{"echo"}}}
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		if r.Header.Get("Authorization") != "test-token" {
			t.Error("missing handshake header")
		}
		c, err := a.Accept(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.CloseNow() }()
		for {
			typ, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			if err := c.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	d := &websocketx.Dialer{Options: websocket.DialOptions{Subprotocols: []string{"echo"}}}
	c, err := d.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{"Authorization": {"test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.CloseNow() }()
	if c.Subprotocol() != "echo" {
		t.Fatalf("subprotocol=%q", c.Subprotocol())
	}
	for _, typ := range []ws.MessageType{ws.Text, ws.Binary} {
		if err := c.Write(ctx, typ, []byte("hello")); err != nil {
			t.Fatal(err)
		}
		gotType, data, err := c.Read(ctx)
		if err != nil || gotType != typ || string(data) != "hello" {
			t.Fatalf("type=%v data=%q error=%v", gotType, data, err)
		}
	}
	if err := c.Write(ctx, 3, nil); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("invalid type: %v", err)
	}
	if err := c.Write(ctx, ws.Text, []byte{0xff}); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("invalid text: %v", err)
	}
	// A reader remains active while Ping waits for the peer's pong.
	readCtx, stopRead := context.WithCancel(ctx)
	readDone := make(chan error, 1)
	go func() { _, _, err := c.Read(readCtx); readDone <- err }()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ws.NormalClosure, "done"); err != nil {
		t.Fatal(err)
	}
	stopRead()
	select {
	case err := <-readDone:
		if err == nil {
			t.Error("Read succeeded after closing the connection")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestOriginPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		allowed []string
		success bool
	}{{"same origin default", nil, false}, {"explicit allowed origin", []string{"allowed.example"}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := &websocketx.Acceptor{Options: websocket.AcceptOptions{OriginPatterns: tc.allowed}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := a.Accept(w, r)
				if err == nil {
					_ = c.CloseNow()
				}
			}))
			defer server.Close()
			c, err := new(websocketx.Dialer).Dial(t.Context(), server.URL, http.Header{"Origin": {"https://allowed.example"}})
			if (err == nil) != tc.success {
				t.Fatalf("success=%v error=%v", tc.success, err)
			}
			if c != nil {
				_ = c.CloseNow()
			}
		})
	}
}

func TestReadLimitAndCloseStatus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		c, err := (&websocketx.Acceptor{ReadLimit: 8}).Accept(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.CloseNow() }()
		if _, _, err := c.Read(r.Context()); err == nil {
			t.Error("oversized message accepted")
		}
	}))
	defer server.Close()
	c, err := new(websocketx.Dialer).Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.CloseNow() }()
	if err := c.Write(ctx, ws.Binary, []byte(strings.Repeat("x", 64))); err != nil {
		t.Fatal(err)
	}
	_, _, err = c.Read(ctx)
	closed, ok := errors.AsType[ws.CloseError](err)
	if !ok || closed.CloseStatus() != 1009 {
		t.Fatalf("close error=%v", err)
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		c, err := new(websocketx.Acceptor).Accept(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.CloseNow() }()
		_, _, _ = c.Read(r.Context())
	}))
	defer server.Close()
	c, err := new(websocketx.Dialer).Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.CloseNow() }()
	readCtx, stopRead := context.WithCancel(ctx)
	stopRead()
	_, _, err = c.Read(readCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("read error=%v", err)
	}
	// A pre-canceled operation may return before touching the connection.
	// The caller still owns it and must explicitly release it.
	_ = c.CloseNow()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := new(websocketx.Dialer).Dial(readCtx, server.URL, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("dial error=%v", err)
	}
}
