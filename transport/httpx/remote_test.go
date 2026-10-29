package httpx_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

// retryRetryable is a test retry middleware: it makes up to attempts calls
// while the error is retryable.
func retryRetryable(attempts int) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			var err error
			for range attempts {
				var resp any
				if resp, err = next(ctx, req); err == nil || !errorx.IsRetryable(err) {
					return resp, err
				}
			}
			return nil, err
		}
	}
}

// TestRetryIsNotMultipliedAcrossHops chains front -> middle -> bottom, where
// both front and middle call the next service through a retrying client.
// Only the hop that received the failure retries it: the bottom service is
// called 3 times rather than 9.
func TestRetryIsNotMultipliedAcrossHops(t *testing.T) {
	quiet := httpx.WithLogger(testLogger(slog.New(slog.DiscardHandler)))
	var bottomCalls atomic.Int32
	bottom := httpx.NewServer(quiet)
	bottom.Handle("POST /work", func(context.Context, any) (any, error) {
		bottomCalls.Add(1)
		return nil, errorx.New(errorx.Unavailable, "BUSY", "busy")
	}, nil)

	forward := func(next http.Handler, wrap bool) *httpx.Server {
		c := newTestClient(t, next, "http://example.com", httpx.WithClientMiddleware(retryRetryable(3)))
		call := c.Endpoint("POST", "/work", nil)
		s := httpx.NewServer(quiet)
		s.Handle("POST /work", func(ctx context.Context, req any) (any, error) {
			_, err := call(ctx, req)
			if wrap {
				// Wrapping with %w keeps the remote mark visible to errorx.From.
				err = fmt.Errorf("call next service: %w", err)
			}
			return nil, err
		}, nil)
		return s
	}
	middle := forward(bottom, true)
	front := forward(middle, false)

	c := newTestClient(t, front, "http://example.com")
	_, err := c.Endpoint("POST", "/work", nil)(t.Context(), nil)
	if e := errorx.From(err); e.Code != errorx.Unavailable || e.Reason != "BUSY" {
		t.Fatalf("err = %v, want Unavailable BUSY", err)
	}
	if n := bottomCalls.Load(); n != 3 {
		t.Errorf("bottom service called %d times, want 3", n)
	}
	if errorx.IsRetryable(err) {
		t.Error("an error that already crossed a retrying hop must arrive as not retryable")
	}
}

func TestClientMarksErrorsRemote(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"errorx body", `{"code":"Unavailable","message":"m","retryable":true}`},
		{"status only", `upstream busy`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, &capture{status: http.StatusServiceUnavailable, contentType: "application/json", body: tc.body}, "http://example.com")
			_, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil)
			e := errorx.From(err)
			if !e.Remote() || !e.Retryable() || e.PropagatedRetryable() {
				t.Errorf("err = %v: remote %v, retryable %v, propagated %v; want true, true, false",
					err, e.Remote(), e.Retryable(), e.PropagatedRetryable())
			}
		})
	}
}

func TestServerWritesPropagatedRetryable(t *testing.T) {
	remote := errorx.New(errorx.Unavailable, "", "busy").WithRemote()
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"remote", remote, false},
		{"wrapped remote", fmt.Errorf("wrap: %w", remote), false},
		{"local decision on remote", remote.WithRetryable(true), true},
		{"local", errors.New("plain"), false},
		{"local retryable", errorx.New(errorx.Unavailable, "", "busy"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httpx.NewServer(httpx.WithLogger(testLogger(slog.New(slog.DiscardHandler))))
			s.Handle("GET /fail", func(context.Context, any) (any, error) { return nil, tc.err }, nil)
			w := do(s, httptest.NewRequest(http.MethodGet, "/fail", nil))
			body := decodeJSON[struct {
				Retryable *bool `json:"retryable"`
			}](t, w)
			if body.Retryable == nil || *body.Retryable != tc.retryable {
				t.Errorf("retryable = %v, want %t; body = %s", body.Retryable, tc.retryable, w.Body)
			}
		})
	}
}
