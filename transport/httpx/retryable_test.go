package httpx_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestErrorBodyCarriesRetryable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"code default retryable", errorx.New(errorx.Unavailable, "", "later"), true},
		{"code default not retryable", errorx.New(errorx.NotFound, "", "gone"), false},
		{"explicitly not retryable", errorx.New(errorx.Unavailable, "", "later").WithRetryable(false), false},
		{"explicitly retryable", errorx.New(errorx.Internal, "", "flaky").WithRetryable(true), true},
		{"converted", errors.New("db exploded"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httpx.NewServer()
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

func TestClientDecodesRetryable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		code      errorx.Code
		retryable bool
	}{
		{"explicitly not retryable", 503, `{"code":"Unavailable","message":"m","retryable":false}`, errorx.Unavailable, false},
		{"explicitly retryable", 500, `{"code":"Internal","message":"m","retryable":true}`, errorx.Internal, true},
		{"absent uses code default", 503, `{"code":"Unavailable","message":"m"}`, errorx.Unavailable, true},
		{"null uses code default", 404, `{"code":"NotFound","message":"m","retryable":null}`, errorx.NotFound, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, &capture{status: tc.status, contentType: "application/json", body: tc.body}, "http://example.com")
			_, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil)
			if errorx.CodeOf(err) != tc.code || errorx.IsRetryable(err) != tc.retryable {
				t.Errorf("err = %v retryable=%t, want %v retryable=%t", err, errorx.IsRetryable(err), tc.code, tc.retryable)
			}
		})
	}

	t.Run("round trip", func(t *testing.T) {
		s := httpx.NewServer()
		s.Handle("POST /fail", func(context.Context, any) (any, error) {
			return nil, errorx.New(errorx.Unavailable, "BUSY", "busy").WithRetryable(false)
		}, nil)
		c := newTestClient(t, s, "http://example.com")
		_, err := httpx.Call[userReply](c, "POST", "/fail")(t.Context(), nil)
		if e := errorx.From(err); e.Code != errorx.Unavailable || e.Reason != "BUSY" || e.Retryable() {
			t.Errorf("err = %v retryable=%t, want non-retryable Unavailable BUSY", err, e.Retryable())
		}
	})
}

// progressTransport fails every request after reporting the given
// connection progress through the request's client trace.
func progressTransport(getConn, gotConn bool, err error) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		if tr := httptrace.ContextClientTrace(r.Context()); tr != nil {
			if getConn && tr.GetConn != nil {
				tr.GetConn(r.URL.Host)
			}
			if gotConn && tr.GotConn != nil {
				tr.GotConn(httptrace.GotConnInfo{})
			}
		}
		return nil, err
	}
}

// setHeader is client middleware that adds an outbound request header.
func setHeader(key, value string) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			if info, ok := metadata.FromContext(ctx); ok {
				info.Header.Set(key, value)
			}
			return next(ctx, req)
		}
	}
}

func TestClientTransportErrorRetryability(t *testing.T) {
	reset := errors.New("connection reset by peer")
	for _, tc := range []struct {
		name      string
		method    string
		getConn   bool
		gotConn   bool
		header    string
		retryable bool
	}{
		{name: "POST before connection", method: "POST", getConn: true, retryable: true},
		{name: "POST after connection", method: "POST", getConn: true, gotConn: true},
		{name: "POST without connection events", method: "POST"},
		{name: "PUT after connection", method: "PUT", getConn: true, gotConn: true},
		{name: "PATCH after connection", method: "PATCH", getConn: true, gotConn: true},
		{name: "DELETE after connection", method: "DELETE", getConn: true, gotConn: true},
		{name: "POST with Idempotency-Key", method: "POST", getConn: true, gotConn: true, header: "Idempotency-Key", retryable: true},
		{name: "POST with X-Idempotency-Key", method: "POST", getConn: true, gotConn: true, header: "X-Idempotency-Key", retryable: true},
		{name: "GET after connection", method: "GET", getConn: true, gotConn: true, retryable: true},
		{name: "HEAD after connection", method: "HEAD", getConn: true, gotConn: true, retryable: true},
		{name: "OPTIONS after connection", method: "OPTIONS", getConn: true, gotConn: true, retryable: true},
		{name: "GET without connection events", method: "GET", retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hc := &http.Client{Transport: progressTransport(tc.getConn, tc.gotConn, reset)}
			c, err := httpx.NewClient(t.Context(), "http://example.com", httpx.WithHTTPClient(hc))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var mw []endpoint.Middleware
			if tc.header != "" {
				mw = append(mw, setHeader(tc.header, "k1"))
			}
			_, err = c.Endpoint(tc.method, "/", nil, mw...)(t.Context(), nil)
			if errorx.CodeOf(err) != errorx.Unavailable || errorx.IsRetryable(err) != tc.retryable {
				t.Errorf("err = %v retryable=%t, want Unavailable retryable=%t", err, errorx.IsRetryable(err), tc.retryable)
			}
			if !errors.Is(err, reset) {
				t.Errorf("err = %v, want the transport error in its chain", err)
			}
		})
	}
}

func TestClientTransportErrorOverNetwork(t *testing.T) {
	t.Run("refused connection is retryable", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		c, err := httpx.NewClient(t.Context(), "http://"+addr, httpx.WithHTTPClient(&http.Client{Transport: &http.Transport{}}))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_, err = c.Endpoint("POST", "/", nil)(t.Context(), nil)
		if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
			t.Errorf("err = %v, want retryable Unavailable", err)
		}
	})

	for _, tc := range []struct {
		method    string
		retryable bool
	}{
		{"POST", false},
		{"GET", true},
	} {
		t.Run(tc.method+" reset after delivery", func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}))
			defer ts.Close()
			tr := &http.Transport{}
			defer tr.CloseIdleConnections()
			c, err := httpx.NewClient(t.Context(), ts.URL, httpx.WithHTTPClient(&http.Client{Transport: tr}))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_, err = c.Endpoint(tc.method, "/", nil)(t.Context(), map[string]string{"name": "alice"})
			if errorx.CodeOf(err) != errorx.Unavailable || errorx.IsRetryable(err) != tc.retryable {
				t.Errorf("err = %v retryable=%t, want Unavailable retryable=%t", err, errorx.IsRetryable(err), tc.retryable)
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("server calls = %d, want 1", n)
			}
		})
	}
}
