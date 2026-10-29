package httpx_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"
)

// userReq exercises every binding source.
type userReq struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Age    int      `json:"age"`
	Tags   []string `json:"tags"`
	Active bool     `json:"active"`
	Score  float64  `json:"score"`
	Limit  *int     `json:"limit"`
	File   *multipart.FileHeader
	Files  []*multipart.FileHeader `json:"files"`
}

// echo returns the request it receives.
func echo(_ context.Context, req any) (any, error) { return req, nil }

// do sends r to h and returns the recorded response.
func do(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func decodeJSON[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return v
}

// errBody mirrors the error body written by the server.
type errBody struct {
	Code     string            `json:"code"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata"`
}

func TestServerLifecycle(t *testing.T) {
	s := httpx.NewServer(httpx.WithAddress("127.0.0.1:0"))
	httpx.Route[userReq](s, "GET /v1/users/{id}", echo)
	if s.Name() != "http" {
		t.Errorf("Name = %q", s.Name())
	}
	if s.URL() != nil {
		t.Errorf("URL before Start = %v, want nil", s.URL())
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}

	s = httpx.NewServer(httpx.WithAddress("127.0.0.1:0"))
	httpx.Route[userReq](s, "GET /v1/users/{id}", echo)
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(context.Background()); err != nil {
			t.Errorf("cleanup Stop: %v", err)
		}
	})
	u := s.URL()
	if u.Scheme != "http" || u.Port() == "" || u.Port() == "0" || u.Hostname() != "127.0.0.1" {
		t.Fatalf("URL = %v", u)
	}
	u.Host = "mutated"
	if s.URL().Host == "mutated" {
		t.Error("URL returned an alias of internal state")
	}
	if err := s.Start(t.Context()); err == nil {
		t.Error("second Start succeeded")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Get(s.URL().String() + "/v1/users/7")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"id":"7"`) {
		t.Fatalf("response %d %s", resp.StatusCode, body)
	}

	// A second server on the same address fails to start.
	dup := httpx.NewServer(httpx.WithAddress(s.URL().Host))
	if err := dup.Start(t.Context()); err == nil {
		_ = dup.Stop(t.Context()) // best-effort cleanup of the unexpected server
		t.Error("Start on a used address succeeded")
	}

	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err, ok := <-s.Err(); ok {
		t.Errorf("Err after Stop = %v, want closed channel", err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Errorf("second Stop: %v", err)
	}
	if err := s.Start(t.Context()); err == nil {
		t.Error("Start after Stop succeeded")
	}
}

func TestBindingOrder(t *testing.T) {
	s := httpx.NewServer()
	httpx.Route[userReq](s, "POST /v1/users/{id}", echo)

	body := `{"id":"body","name":"body","age":30,"tags":["body"],"active":true,"score":1.5}`
	r := httptest.NewRequest(http.MethodPost, "/v1/users/path?name=query&tags=a&tags=b&limit=5", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := do(s, r)
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	got := decodeJSON[userReq](t, w)
	if got.ID != "path" || got.Name != "query" || got.Age != 30 || !slices.Equal(got.Tags, []string{"a", "b"}) ||
		!got.Active || got.Score != 1.5 || got.Limit == nil || *got.Limit != 5 {
		t.Errorf("bound = %+v", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestBindingForms(t *testing.T) {
	s := httpx.NewServer()
	var bound *userReq
	httpx.Route[userReq](s, "POST /v1/users/{id}", func(_ context.Context, req any) (any, error) {
		bound = req.(*userReq)
		return nil, nil
	})

	t.Run("urlencoded", func(t *testing.T) {
		form := url.Values{"id": {"form"}, "name": {"form"}, "age": {"7"}, "tags": {"x", "y"}, "active": {"true"}}
		r := httptest.NewRequest(http.MethodPost, "/v1/users/42?name=query", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if w := do(s, r); w.Code != http.StatusNoContent {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		if bound.ID != "42" || bound.Name != "query" || bound.Age != 7 || !slices.Equal(bound.Tags, []string{"x", "y"}) || !bound.Active {
			t.Errorf("bound = %+v", bound)
		}
	})

	t.Run("multipart", func(t *testing.T) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for _, kv := range [][2]string{{"name", "multi"}, {"score", "2.25"}} {
			if err := mw.WriteField(kv[0], kv[1]); err != nil {
				t.Fatal(err)
			}
		}
		for _, f := range []struct{ field, name, content string }{
			{"File", "a.txt", "alpha"},
			{"files", "b.txt", "beta"},
			{"files", "c.txt", "gamma"},
		} {
			fw, err := mw.CreateFormFile(f.field, f.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(fw, f.content); err != nil {
				t.Fatal(err)
			}
		}
		mw.Close()
		r := httptest.NewRequest(http.MethodPost, "/v1/users/9", &buf)
		r.Header.Set("Content-Type", mw.FormDataContentType())
		if w := do(s, r); w.Code != http.StatusNoContent {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		if bound.ID != "9" || bound.Name != "multi" || bound.Score != 2.25 {
			t.Errorf("bound = %+v", bound)
		}
		if bound.File == nil || bound.File.Filename != "a.txt" {
			t.Fatalf("File = %+v", bound.File)
		}
		f, err := bound.File.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, _ := io.ReadAll(f)
		f.Close()
		if string(content) != "alpha" {
			t.Errorf("File content = %q", content)
		}
		if len(bound.Files) != 2 || bound.Files[0].Filename != "b.txt" || bound.Files[1].Filename != "c.txt" {
			t.Errorf("Files = %v", bound.Files)
		}
	})
}

func TestBindingErrors(t *testing.T) {
	s := httpx.NewServer()
	called := false
	httpx.Route[userReq](s, "POST /v1/users/{id}", func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	})
	httpx.Route[struct {
		ID int `json:"id"`
	}](s, "GET /v1/items/{id}", echo)

	tests := []struct {
		name        string
		method      string
		target      string
		contentType string
		body        string
		reason      string
	}{
		{"malformed json", "POST", "/v1/users/1", "application/json", `{"name":`, "INVALID_BODY"},
		{"wrong json type", "POST", "/v1/users/1", "application/json", `{"age":"x"}`, "INVALID_BODY"},
		{"unsupported media type", "POST", "/v1/users/1", "text/csv", "a,b", "UNSUPPORTED_MEDIA_TYPE"},
		{"malformed content type", "POST", "/v1/users/1", "application/", "{}", "INVALID_CONTENT_TYPE"},
		{"bad query int", "POST", "/v1/users/1?age=old", "", "", "INVALID_PARAMETER"},
		{"bad query bool", "POST", "/v1/users/1?active=maybe", "", "", "INVALID_PARAMETER"},
		{"bad form value", "POST", "/v1/users/1", "application/x-www-form-urlencoded", "age=x", "INVALID_PARAMETER"},
		{"bad multipart", "POST", "/v1/users/1", "multipart/form-data; boundary=zzz", "garbage", "INVALID_BODY"},
		{"bad path int", "GET", "/v1/items/abc", "", "", "INVALID_PARAMETER"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			r := httptest.NewRequest(tt.method, tt.target, strings.NewReader(tt.body))
			if tt.contentType != "" {
				r.Header.Set("Content-Type", tt.contentType)
			}
			w := do(s, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			got := decodeJSON[errBody](t, w)
			if got.Code != "InvalidArgument" || got.Reason != tt.reason {
				t.Errorf("error body = %+v, want reason %s", got, tt.reason)
			}
			if called {
				t.Error("endpoint called despite binding error")
			}
		})
	}
}

// textCodec is a test codec named "text" that renders values with fmt.
type textCodec struct{}

func (textCodec) Name() string                  { return "text" }
func (textCodec) Marshal(v any) ([]byte, error) { return fmt.Appendf(nil, "text:%v", v), nil }
func (textCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*map[string]string)
	if !ok {
		return errors.New("textCodec: unsupported target")
	}
	*p = map[string]string{"raw": string(data)}
	return nil
}

func TestCodecNegotiation(t *testing.T) {
	s := httpx.NewServer(httpx.WithCodecs(textCodec{}, nil))
	httpx.Route[map[string]string](s, "POST /echo", func(_ context.Context, req any) (any, error) {
		m := *req.(*map[string]string)
		if m == nil {
			m = map[string]string{}
		}
		return m, nil
	})

	tests := []struct {
		name        string
		accept      []string
		contentType string
		body        string
		wantType    string
		wantBody    string
	}{
		{"default json", nil, "", "", "application/json", "{}"},
		{"json body", nil, "application/json", `{"a":"b"}`, "application/json", `{"a":"b"}`},
		{"accept text", []string{"application/text"}, "", "", "application/text", "text:map[]"},
		{"accept x- prefix", []string{"application/x-text"}, "", "", "application/text", "text:map[]"},
		{"request content type", nil, "application/text", "hi", "application/text", "text:map[raw:hi]"},
		{"accept wins over content type", []string{"application/json"}, "application/text", "hi", "application/json", `{"raw":"hi"}`},
		{"q values", []string{"text/html, application/text;q=0.5, application/json;q=0.9"}, "", "", "application/json", "{}"},
		{"multiple accept headers", []string{"application/json;q=0.1", "application/text"}, "", "", "application/text", "text:map[]"},
		{"q zero excluded", []string{"application/text;q=0"}, "", "", "application/json", "{}"},
		{"wildcard", []string{"*/*"}, "", "", "application/json", "{}"},
		{"unknown accept", []string{"application/xml"}, "", "", "application/json", "{}"},
		{"bad accept entries", []string{"/;;, application/text;q=abc"}, "", "", "application/json", "{}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(tt.body))
			for _, a := range tt.accept {
				r.Header.Add("Accept", a)
			}
			if tt.contentType != "" {
				r.Header.Set("Content-Type", tt.contentType)
			}
			w := do(s, r)
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if ct := w.Header().Get("Content-Type"); ct != tt.wantType {
				t.Errorf("Content-Type = %q, want %q", ct, tt.wantType)
			}
			if got := w.Body.String(); got != tt.wantBody {
				t.Errorf("body = %q, want %q", got, tt.wantBody)
			}
		})
	}
}

func TestNilReplyIs204(t *testing.T) {
	s := httpx.NewServer()
	s.Handle("DELETE /a", func(context.Context, any) (any, error) { return nil, nil }, nil)
	s.Handle("DELETE /b", func(context.Context, any) (any, error) { return (*userReq)(nil), nil }, nil)
	s.Handle("GET /c", func(context.Context, any) (any, error) { return []string(nil), nil }, nil)
	for _, path := range []string{"/a", "/b"} {
		w := do(s, httptest.NewRequest(http.MethodDelete, path, nil))
		if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
			t.Errorf("%s: status %d body %q, want 204", path, w.Code, w.Body)
		}
	}
	if w := do(s, httptest.NewRequest(http.MethodGet, "/c", nil)); w.Code != 200 {
		t.Errorf("nil slice: status %d, want 200", w.Code)
	}
}

func TestErrorResponses(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		body   errBody
	}{
		{"not found", errorx.New(errorx.NotFound, "USER_NOT_FOUND", "user 1 not found").WithMetadata(map[string]string{"id": "1"}),
			404, errBody{"NotFound", "USER_NOT_FOUND", "user 1 not found", map[string]string{"id": "1"}}},
		{"wrapped", fmt.Errorf("lookup: %w", errorx.New(errorx.PermissionDenied, "NO", "denied")),
			403, errBody{Code: "PermissionDenied", Reason: "NO", Message: "denied"}},
		{"plain", errors.New("db exploded"), 500, errBody{Code: "Unknown", Message: "unknown error"}},
		{"canceled", context.Canceled, 499, errBody{Code: "Canceled", Message: "operation canceled"}},
		{"deadline", context.DeadlineExceeded, 504, errBody{Code: "DeadlineExceeded", Message: "deadline exceeded"}},
		{"unavailable", errorx.New(errorx.Unavailable, "", "later"), 503, errBody{Code: "Unavailable", Message: "later"}},
		{"conflict", errorx.New(errorx.Aborted, "", "retry"), 409, errBody{Code: "Aborted", Message: "retry"}},
		{"unauthenticated", errorx.New(errorx.Unauthenticated, "", "who"), 401, errBody{Code: "Unauthenticated", Message: "who"}},
		{"rate limited", errorx.New(errorx.ResourceExhausted, "", "slow"), 429, errBody{Code: "ResourceExhausted", Message: "slow"}},
		{"unimplemented", errorx.New(errorx.Unimplemented, "", "no"), 501, errBody{Code: "Unimplemented", Message: "no"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := httpx.NewServer()
			s.Handle("GET /fail", func(context.Context, any) (any, error) { return nil, tt.err }, nil)
			w := do(s, httptest.NewRequest(http.MethodGet, "/fail", nil))
			if w.Code != tt.status {
				t.Errorf("status = %d, want %d", w.Code, tt.status)
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}
			got := decodeJSON[errBody](t, w)
			if got.Code != tt.body.Code || got.Reason != tt.body.Reason || got.Message != tt.body.Message ||
				len(got.Metadata) != len(tt.body.Metadata) || got.Metadata["id"] != tt.body.Metadata["id"] {
				t.Errorf("body = %+v, want %+v", got, tt.body)
			}
			if tt.body.Metadata == nil && !strings.Contains(w.Body.String(), `"metadata":{}`) {
				t.Errorf("empty metadata must be serialized as an empty object: %s", w.Body)
			}
		})
	}
}

func TestErrorBodyIsJSONRegardlessOfAccept(t *testing.T) {
	s := httpx.NewServer(httpx.WithCodecs(textCodec{}))
	s.Handle("GET /fail", func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.NotFound, "X", "y")
	}, nil)
	r := httptest.NewRequest(http.MethodGet, "/fail", nil)
	r.Header.Set("Accept", "application/text")
	w := do(s, r)
	if w.Code != 404 || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d type %q", w.Code, w.Header().Get("Content-Type"))
	}
}

func TestEncodeFailureIs500(t *testing.T) {
	s := httpx.NewServer()
	s.Handle("GET /bad", func(context.Context, any) (any, error) { return make(chan int), nil }, nil)
	w := do(s, httptest.NewRequest(http.MethodGet, "/bad", nil))
	if w.Code != 500 {
		t.Fatalf("status %d", w.Code)
	}
	if got := decodeJSON[errBody](t, w); got.Code != "Internal" || got.Reason != "ENCODE_REPLY" {
		t.Errorf("body = %+v", got)
	}
}

func TestHealthProbes(t *testing.T) {
	var ready error
	s := httpx.NewServer(httpx.WithHealth(health.CheckerFunc(func(context.Context) error { return ready })))
	tests := []struct {
		path   string
		ready  error
		status int
	}{
		{"/livez", nil, 200},
		{"/livez", errors.New("down"), 200},
		{"/readyz", nil, 200},
		{"/readyz", errors.New("down"), 503},
	}
	for _, tt := range tests {
		ready = tt.ready
		for _, method := range []string{"GET", "HEAD"} {
			if w := do(s, httptest.NewRequest(method, tt.path, nil)); w.Code != tt.status {
				t.Errorf("%s %s (ready=%v) = %d, want %d", method, tt.path, tt.ready, w.Code, tt.status)
			}
		}
	}
	if w := do(s, httptest.NewRequest(http.MethodPost, "/livez", nil)); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /livez = %d, want 405", w.Code)
	}

	plain := httpx.NewServer()
	for _, path := range []string{"/livez", "/readyz"} {
		if w := do(plain, httptest.NewRequest(http.MethodGet, path, nil)); w.Code != http.StatusNotFound {
			t.Errorf("GET %s without Health = %d, want 404", path, w.Code)
		}
	}
}

func TestServerMetadataAndMiddleware(t *testing.T) {
	var order []string
	var infos []metadata.Info
	mark := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				order = append(order, name)
				info, _ := metadata.FromContext(ctx)
				infos = append(infos, info)
				return next(ctx, req)
			}
		}
	}
	s := httpx.NewServer(httpx.WithMiddleware(mark("outer"), nil, mark("inner")))
	var handlerInfo metadata.Info
	ep := func(ctx context.Context, req any) (any, error) {
		order = append(order, "handler")
		handlerInfo, _ = metadata.FromContext(ctx)
		return nil, nil
	}
	s.Handle("GET /v1/users/{id}", ep, nil)
	httpx.Route[userReq](s, "PUT /v1/users/{id}", ep, httpx.WithOperation("users.Update"))
	s.HandleHTTP("GET /raw", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		order = append(order, "raw")
		w.WriteHeader(http.StatusTeapot)
	}))

	r := httptest.NewRequest(http.MethodGet, "/v1/users/1", nil)
	r.Header.Set("X-Request-ID", "abc")
	r.Header.Add("X-Multi", "1")
	r.Header.Add("X-Multi", "2")
	if w := do(s, r); w.Code != http.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	if !slices.Equal(order, []string{"outer", "inner", "handler"}) {
		t.Errorf("order = %v", order)
	}
	if handlerInfo.Kind != metadata.KindServer || handlerInfo.Protocol != "http" || handlerInfo.Operation != "GET /v1/users/{id}" {
		t.Errorf("info = %+v", handlerInfo)
	}
	if handlerInfo.Header.Get("x-request-id") != "abc" || !slices.Equal(handlerInfo.Header.Values("x-multi"), []string{"1", "2"}) {
		t.Errorf("header = %v", handlerInfo.Header)
	}
	if infos[0].Operation != handlerInfo.Operation {
		t.Errorf("middleware info = %+v", infos[0])
	}

	order = nil
	if w := do(s, httptest.NewRequest(http.MethodPut, "/v1/users/1", nil)); w.Code != http.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	if handlerInfo.Operation != "users.Update" {
		t.Errorf("operation = %q", handlerInfo.Operation)
	}

	order = nil
	if w := do(s, httptest.NewRequest(http.MethodGet, "/raw", nil)); w.Code != http.StatusTeapot {
		t.Fatalf("raw status %d", w.Code)
	}
	if !slices.Equal(order, []string{"raw"}) {
		t.Errorf("HandleHTTP order = %v, want middleware skipped", order)
	}
}

func TestNilNewReq(t *testing.T) {
	s := httpx.NewServer()
	var got any = "unset"
	s.Handle("POST /x", func(_ context.Context, req any) (any, error) {
		got = req
		return map[string]int{"n": 1}, nil
	}, nil)
	r := httptest.NewRequest(http.MethodPost, "/x?a=1", strings.NewReader(`{"a":1}`))
	w := do(s, r)
	if w.Code != 200 || got != nil {
		t.Errorf("status %d req %v", w.Code, got)
	}
}
