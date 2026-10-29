package httpx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// captured is the last request seen by a capture server.
type captured struct {
	method string
	path   string // escaped
	query  string
	header http.Header
	body   string
}

// capture is an http.Handler that records requests and answers with a
// fixed response.
type capture struct {
	status      int
	contentType string
	body        string

	mu   sync.Mutex
	last captured
}

func (c *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.last = captured{
		method: r.Method,
		path:   r.URL.EscapedPath(),
		query:  r.URL.RawQuery,
		header: r.Header.Clone(),
		body:   string(body),
	}
	c.mu.Unlock()
	if c.contentType != "" {
		w.Header().Set("Content-Type", c.contentType)
	}
	w.WriteHeader(c.status)
	// A failed write surfaces as a client error in the test itself.
	_, _ = io.WriteString(w, c.body)
}

func (c *capture) request() captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// newTestClient starts h on an in-memory test server and returns a client
// for target that talks to it.
func newTestClient(t *testing.T, h http.Handler, target string, opts ...httpx.ClientOption) *httpx.Client {
	t.Helper()
	ts := httptest.NewTestServer(t, h)
	opts = append([]httpx.ClientOption{httpx.WithHTTPClient(ts.Client())}, opts...)
	c, err := httpx.NewClient(t.Context(), target, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type getUserReq struct {
	ID    int      `json:"id"`
	Path  string   `json:"path"`
	Q     string   `json:"q"`
	Tags  []string `json:"tags"`
	Empty *string  `json:"empty"`
}

type userReply struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestClientRequestEncoding(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		target    string
		req       any
		wantPath  string
		wantQuery string
		wantBody  string
	}{
		{
			name: "get with path and query", method: "get", path: "/v1/users/{id}/files/{path...}",
			target:   "http://example.com",
			req:      &getUserReq{ID: 42, Path: "a b/c", Q: "x&y", Tags: []string{"t1", "t2"}},
			wantPath: "/v1/users/42/files/a%20b/c", wantQuery: "q=x%26y&tags=t1&tags=t2",
		},
		{
			name: "delete uses query", method: "DELETE", path: "/v1/users/{id}",
			target:   "http://example.com",
			req:      getUserReq{ID: 1, Q: "z"},
			wantPath: "/v1/users/1", wantQuery: "path=&q=z",
		},
		{
			name: "post uses body", method: "POST", path: "/v1/users/{id}",
			target:   "http://example.com",
			req:      &userReply{ID: 5, Name: "neo"},
			wantPath: "/v1/users/5", wantBody: `{"id":5,"name":"neo"}`,
		},
		{
			name: "base path", method: "PUT", path: "/users",
			target:   "http://example.com/api/",
			req:      map[string]string{"name": "x"},
			wantPath: "/api/users", wantBody: `{"name":"x"}`,
		},
		{
			name: "nil request", method: "GET", path: "/ping",
			target:   "http://example.com",
			wantPath: "/ping",
		},
		{
			name: "non-object request", method: "GET", path: "/ping",
			target: "http://example.com", req: []int{1},
			wantPath: "/ping",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &capture{status: 200, contentType: "application/json", body: `{"id":1,"name":"a"}`}
			c := newTestClient(t, h, tt.target)
			reply, err := c.Endpoint(tt.method, tt.path, func() any { return new(userReply) })(t.Context(), tt.req)
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if r, ok := reply.(*userReply); !ok || *r != (userReply{ID: 1, Name: "a"}) {
				t.Errorf("reply = %#v", reply)
			}
			got := h.request()
			if got.method != strings.ToUpper(tt.method) || got.path != tt.wantPath || got.query != tt.wantQuery || got.body != tt.wantBody {
				t.Errorf("request = %s %s ?%s body %q; want %s ?%s body %q",
					got.method, got.path, got.query, got.body, tt.wantPath, tt.wantQuery, tt.wantBody)
			}
			if got.header.Get("Accept") != "application/json" {
				t.Errorf("Accept = %q", got.header.Get("Accept"))
			}
			wantCT := ""
			if tt.wantBody != "" {
				wantCT = "application/json"
			}
			if ct := got.header.Get("Content-Type"); ct != wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, wantCT)
			}
		})
	}
}

func TestClientMissingPathParameter(t *testing.T) {
	h := &capture{status: 200}
	c := newTestClient(t, h, "http://example.com")
	for _, req := range []any{nil, map[string]any{"other": 1}, map[string]any{"id": nil}} {
		if _, err := c.Endpoint("GET", "/v1/users/{id}", nil)(t.Context(), req); err == nil {
			t.Errorf("request %v: call succeeded, want error", req)
		}
	}
	if _, err := c.Endpoint("POST", "/x", nil)(t.Context(), make(chan int)); err == nil {
		t.Error("unencodable request succeeded")
	}
}

func TestClientHeadersAndOperation(t *testing.T) {
	h := &capture{status: http.StatusNoContent}
	var info metadata.Info
	setHeader := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, _ = metadata.FromContext(ctx)
			info.Header.Set("x-request-id", "abc")
			info.Header.Add("x-multi", "1")
			info.Header.Add("x-multi", "2")
			info.Header.Set("accept", "application/json;q=0.5")
			return next(ctx, req)
		}
	}
	c := newTestClient(t, h, "http://example.com", httpx.WithClientMiddleware(setHeader))
	reply, err := httpx.Call[userReply](c, "get", "/v1/ping")(t.Context(), nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if r, ok := reply.(*userReply); !ok || *r != (userReply{}) {
		t.Errorf("204 reply = %#v, want zero *userReply", reply)
	}
	if info.Kind != metadata.KindClient || info.Protocol != "http" || info.Operation != "GET /v1/ping" {
		t.Errorf("info = %+v", info)
	}
	got := h.request().header
	if got.Get("X-Request-Id") != "abc" || !slices.Equal(got.Values("X-Multi"), []string{"1", "2"}) {
		t.Errorf("header = %v", got)
	}
	if got.Get("Accept") != "application/json;q=0.5" {
		t.Errorf("Accept = %q, metadata should win", got.Get("Accept"))
	}
}

func TestClientReplyHandling(t *testing.T) {
	t.Run("nil newReply", func(t *testing.T) {
		c := newTestClient(t, &capture{status: 200, body: `{"id":1}`}, "http://example.com")
		reply, err := c.Endpoint("GET", "/", nil)(t.Context(), nil)
		if err != nil || reply != nil {
			t.Errorf("reply = %v, %v", reply, err)
		}
	})
	t.Run("bad reply body", func(t *testing.T) {
		c := newTestClient(t, &capture{status: 200, contentType: "application/json", body: `{"id":`}, "http://example.com")
		if _, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil); err == nil {
			t.Error("decoding a malformed reply succeeded")
		}
	})
	t.Run("json reply with custom codec", func(t *testing.T) {
		c := newTestClient(t, &capture{status: 200, contentType: "application/json", body: `{"name":"j"}`},
			"http://example.com", httpx.WithCodec(textCodec{}))
		reply, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil)
		if err != nil || reply.(*userReply).Name != "j" {
			t.Errorf("reply = %v, %v", reply, err)
		}
	})
	t.Run("custom codec request", func(t *testing.T) {
		h := &capture{status: 200, contentType: "application/text", body: "pong"}
		c := newTestClient(t, h, "http://example.com", httpx.WithCodec(textCodec{}))
		reply, err := c.Endpoint("POST", "/", func() any { return new(map[string]string) })(t.Context(), "ping")
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		if m := *reply.(*map[string]string); m["raw"] != "pong" {
			t.Errorf("reply = %v", m)
		}
		got := h.request()
		if got.body != "text:ping" || got.header.Get("Content-Type") != "application/text" || got.header.Get("Accept") != "application/text" {
			t.Errorf("request = %+v", got)
		}
	})
}

func TestClientErrorDecoding(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		code        errorx.Code
		reason      string
		message     string
		metadata    map[string]string
	}{
		{"errorx body", 404, "application/json", `{"code":"NotFound","reason":"USER_NOT_FOUND","message":"gone","metadata":{"id":"1"}}`,
			errorx.NotFound, "USER_NOT_FOUND", "gone", map[string]string{"id": "1"}},
		{"errorx body wins over status", 500, "application/json", `{"code":"Aborted","reason":"R","message":"m"}`,
			errorx.Aborted, "R", "m", nil},
		{"plain 503", 503, "text/plain", "try later", errorx.Unavailable, "", "503 Service Unavailable: try later", nil},
		{"empty 404", 404, "", "", errorx.NotFound, "", "404 Not Found", nil},
		{"unknown code falls back", 409, "application/json", `{"code":"Bogus","message":"x"}`,
			errorx.AlreadyExists, "", `409 Conflict: {"code":"Bogus","message":"x"}`, nil},
		{"ok code falls back", 400, "application/json", `{"code":"OK"}`, errorx.InvalidArgument, "", `400 Bad Request: {"code":"OK"}`, nil},
		{"teapot", 418, "", "", errorx.Unknown, "", "418 I'm a teapot", nil},
		{"redirect is an error", 304, "", "", errorx.Unknown, "", "304 Not Modified", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, &capture{status: tt.status, contentType: tt.contentType, body: tt.body}, "http://example.com")
			_, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil)
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok {
				t.Fatalf("err = %v, want *errorx.Error", err)
			}
			if e.Code != tt.code || e.Reason != tt.reason || e.Message != tt.message ||
				len(e.Metadata) != len(tt.metadata) || e.Metadata["id"] != tt.metadata["id"] {
				t.Errorf("err = %+v, want %v %q %q %v", e, tt.code, tt.reason, tt.message, tt.metadata)
			}
		})
	}

	t.Run("long body truncated", func(t *testing.T) {
		c := newTestClient(t, &capture{status: 500, body: strings.Repeat("x", 4096)}, "http://example.com")
		_, err := httpx.Call[userReply](c, "GET", "/")(t.Context(), nil)
		if e := errorx.From(err); len(e.Message) > 600 {
			t.Errorf("message length %d", len(e.Message))
		}
	})
}

func TestClientTransportError(t *testing.T) {
	ts := httptest.NewTestServer(t, http.NotFoundHandler())
	ctx, cancel := context.WithCancel(t.Context())
	c, err := httpx.NewClient(ctx, "http://example.com", httpx.WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	_, err = c.Endpoint("GET", "/", nil)(ctx, nil)
	if errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("err = %v, want Canceled", err)
	}
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientConnectionErrorIsUnavailable(t *testing.T) {
	refused := errors.New("connection refused")
	hc := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, refused
	})}
	c, err := httpx.NewClient(t.Context(), "http://example.com", httpx.WithHTTPClient(hc))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Endpoint("GET", "/", nil)(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
		t.Errorf("err = %v, want retryable Unavailable", err)
	}
	if !errors.Is(err, refused) {
		t.Errorf("err = %v, want the transport error in its chain", err)
	}

	hc.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	_, err = c.Endpoint("GET", "/", nil)(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.DeadlineExceeded {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestNewClientTargets(t *testing.T) {
	for _, target := range []string{"grpc://host:1", "host:1", "http://[::1", "discovery:///svc", "http://"} {
		if c, err := httpx.NewClient(t.Context(), target); err == nil {
			c.Close()
			t.Errorf("NewClient(%q) succeeded, want error", target)
		}
	}
	c, err := httpx.NewClient(t.Context(), "https://example.com", httpx.WithHTTPClient(nil), httpx.WithCodec(nil), httpx.WithSelector(nil))
	if err != nil {
		t.Fatalf("NewClient(https): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// Compile-time check that the test codec is a Codec.
var _ encoding.Codec = textCodec{}
