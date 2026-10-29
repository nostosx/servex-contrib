package httpx_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestResponseEncoder(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer()
	s.Handle("POST /items", func(context.Context, any) (any, error) {
		return "created item", nil
	}, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, r *http.Request, reply any) error {
		if r.Method != http.MethodPost || reply != "created item" {
			t.Errorf("encoder request %s, reply %v", r.Method, reply)
		}
		w.Header().Set("Location", "/items/7")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Add("X-Item", "first")
		w.Header().Add("X-Item", "second")
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc", HttpOnly: true})
		http.SetCookie(w, &http.Cookie{Name: "theme", Value: "dark"})
		w.WriteHeader(http.StatusCreated)
		_, err := io.WriteString(w, reply.(string))
		return err
	}))
	r := httptest.NewRequest(http.MethodPost, "/items", nil)
	r.Header.Set("Accept", "application/json")
	w := do(s, r)
	if w.Code != http.StatusCreated || w.Body.String() != "created item" {
		t.Fatalf("response %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("Location") != "/items/7" || w.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Errorf("headers = %v", w.Header())
	}
	if !slices.Equal(w.Header().Values("X-Item"), []string{"first", "second"}) {
		t.Errorf("X-Item = %v", w.Header().Values("X-Item"))
	}
	resp := w.Result()
	defer resp.Body.Close()
	cookies := resp.Cookies()
	if len(cookies) != 2 || cookies[0].Name != "session" || cookies[0].Value != "abc" || !cookies[0].HttpOnly || cookies[1].Name != "theme" {
		t.Errorf("cookies = %v", cookies)
	}
}

func TestResponseEncoderNilReplyAndDefault(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		reply any
	}{
		{"nil", nil},
		{"typed nil", (*userReq)(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := httpx.NewServer()
			called := false
			s.Handle("GET /", func(context.Context, any) (any, error) { return tt.reply, nil }, nil,
				httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, reply any) error {
					called = true
					if reply != tt.reply {
						t.Errorf("reply = %v, want %v", reply, tt.reply)
					}
					w.WriteHeader(http.StatusAccepted)
					return nil
				}))
			if w := do(s, httptest.NewRequest(http.MethodGet, "/", nil)); !called || w.Code != http.StatusAccepted || w.Body.Len() != 0 {
				t.Errorf("called %v, response %d %q", called, w.Code, w.Body.String())
			}
		})
	}
	for _, tt := range []struct {
		name   string
		reply  any
		status int
		body   string
	}{
		{"json", map[string]string{"name": "item"}, http.StatusOK, `{"name":"item"}`},
		{"nil", nil, http.StatusNoContent, ""},
	} {
		t.Run("nil encoder/"+tt.name, func(t *testing.T) {
			s := httpx.NewServer()
			s.Handle("GET /", func(context.Context, any) (any, error) { return tt.reply, nil }, nil,
				httpx.WithResponseEncoder(func(http.ResponseWriter, *http.Request, any) error {
					t.Error("replaced encoder called")
					return nil
				}), httpx.WithResponseEncoder(nil))
			if w := do(s, httptest.NewRequest(http.MethodGet, "/", nil)); w.Code != tt.status || w.Body.String() != tt.body {
				t.Errorf("response %d %q, want %d %q", w.Code, w.Body.String(), tt.status, tt.body)
			}
		})
	}
}

func TestResponseEncoderMetadataAndMiddleware(t *testing.T) {
	t.Parallel()
	var order []string
	middleware := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				order = append(order, name+" before")
				reply, err := next(ctx, req)
				order = append(order, name+" after")
				return reply, err
			}
		}
	}
	s := httpx.NewServer(httpx.WithMiddleware(middleware("outer"), middleware("inner")))
	var endpointInfo metadata.Info
	httpx.Route[userReq](s, "GET /items/{id}", func(ctx context.Context, req any) (any, error) {
		order = append(order, "endpoint")
		endpointInfo, _ = metadata.FromContext(ctx)
		return req.(*userReq).ID, nil
	}, httpx.WithOperation("items.Get"), httpx.WithResponseEncoder(func(w http.ResponseWriter, r *http.Request, reply any) error {
		order = append(order, "encoder")
		info, ok := metadata.FromContext(r.Context())
		if !ok || info.Kind != metadata.KindServer || info.Protocol != "http" || info.Operation != "items.Get" || info.Operation != endpointInfo.Operation || info.Header.Get("x-request-id") != "abc" {
			t.Errorf("encoder metadata = %+v (present %v)", info, ok)
		}
		if r.PathValue("id") != "7" || reply != "7" {
			t.Errorf("path %q, reply %v", r.PathValue("id"), reply)
		}
		_, err := io.WriteString(w, reply.(string))
		return err
	}))
	r := httptest.NewRequest(http.MethodGet, "/items/7", nil)
	r.Header.Set("X-Request-ID", "abc")
	w := do(s, r)
	if w.Code != http.StatusOK || w.Body.String() != "7" {
		t.Fatalf("response %d %q", w.Code, w.Body.String())
	}
	want := []string{"outer before", "inner before", "endpoint", "inner after", "outer after", "encoder"}
	if !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestResponseEncoderBypassedOnErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		body   string
		err    error
		status int
		reason string
	}{
		{"endpoint", `{}`, errorx.New(errorx.NotFound, "MISSING", "missing item"), http.StatusNotFound, "MISSING"},
		{"binding", `{`, nil, http.StatusBadRequest, "INVALID_BODY"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := httpx.NewServer()
			httpx.Route[userReq](s, "POST /", func(context.Context, any) (any, error) {
				if tt.err == nil {
					t.Error("endpoint called after binding failure")
				}
				return "ignored reply", tt.err
			}, httpx.WithResponseEncoder(func(http.ResponseWriter, *http.Request, any) error {
				t.Error("encoder called after request failure")
				return nil
			}))
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			w := do(s, r)
			if w.Code != tt.status || decodeJSON[errBody](t, w).Reason != tt.reason {
				t.Errorf("response %d %q", w.Code, w.Body.String())
			}
		})
	}
}

func TestResponseEncoderErrorBeforeCommit(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	s := httpx.NewServer(httpx.WithLogger(testLogger(slog.New(slog.NewTextHandler(&logs, nil)))))
	s.Handle("GET /", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		w.Header().Set("X-Existing", "changed")
		w.Header().Del("X-Preserved")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "99999")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Location", "/partial")
		http.SetCookie(w, &http.Cookie{Name: "partial", Value: "secret"})
		return errorx.New(errorx.NotFound, "SECRET_REASON", "private encoder failure")
	}))
	w := httptest.NewRecorder()
	w.Header()["X-Existing"] = []string{"one", "two"}
	w.Header().Set("X-Preserved", "present")
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	got := decodeJSON[errBody](t, w)
	if w.Code != http.StatusInternalServerError || got.Code != "Internal" || got.Reason != "ENCODE_REPLY" || got.Message != "failed to encode the reply" {
		t.Errorf("response %d %+v", w.Code, got)
	}
	if !slices.Equal(w.Header().Values("X-Existing"), []string{"one", "two"}) || w.Header().Get("X-Preserved") != "present" {
		t.Errorf("original headers = %v", w.Header())
	}
	for _, name := range []string{"Content-Length", "Content-Encoding", "Location", "Set-Cookie"} {
		if got := w.Header().Get(name); got != "" {
			t.Errorf("partial %s survived: %q", name, got)
		}
	}
	if w.Header().Get("Content-Type") != "application/json" || strings.Contains(w.Body.String(), "private") || !strings.Contains(logs.String(), "private encoder failure") {
		t.Errorf("body %q, headers %v, logs %q", w.Body.String(), w.Header(), logs.String())
	}
}

func TestResponseEncoderErrorAfterCommit(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		commit func(http.ResponseWriter) error
		status int
		body   string
	}{
		{"status", func(w http.ResponseWriter) error { w.WriteHeader(http.StatusCreated); return nil }, http.StatusCreated, ""},
		{"switching protocols", func(w http.ResponseWriter) error { w.WriteHeader(http.StatusSwitchingProtocols); return nil }, http.StatusSwitchingProtocols, ""},
		{"write", func(w http.ResponseWriter) error { _, err := io.WriteString(w, "partial"); return err }, http.StatusOK, "partial"},
		{"empty write", func(w http.ResponseWriter) error { _, err := w.Write(nil); return err }, http.StatusOK, ""},
		{"flush", func(w http.ResponseWriter) error { return http.NewResponseController(w).Flush() }, http.StatusOK, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			s := httpx.NewServer(httpx.WithLogger(testLogger(slog.New(slog.NewTextHandler(&logs, nil)))))
			s.Handle("GET /", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
				w.Header().Set("Content-Type", "text/plain")
				if err := tt.commit(w); err != nil {
					t.Errorf("commit: %v", err)
				}
				return errors.New("encoder failed after output")
			}))
			w := do(s, httptest.NewRequest(http.MethodGet, "/", nil))
			if w.Code != tt.status || w.Body.String() != tt.body || w.Header().Get("Content-Type") != "text/plain" {
				t.Errorf("response %d %q, headers %v", w.Code, w.Body.String(), w.Header())
			}
			if !strings.Contains(logs.String(), "encoder failed after output") || !strings.Contains(logs.String(), "operation=\"GET /\"") {
				t.Errorf("logs = %q", logs.String())
			}
		})
	}
}

func TestResponseEncoderInformationalHeaders(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer(httpx.WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	s.Handle("GET /", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusProcessing)
		return errors.New("failed after early hints")
	}))
	server := httptest.NewServer(s)
	defer server.Close()
	var statuses []int
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{
		Got1xxResponse: func(status int, _ textproto.MIMEHeader) error {
			statuses = append(statuses, status)
			return nil
		},
	}))
	resp, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(statuses, []int{http.StatusEarlyHints, http.StatusProcessing}) || resp.StatusCode != http.StatusInternalServerError || !strings.Contains(string(body), `"reason":"ENCODE_REPLY"`) || resp.Header.Get("Link") != "" {
		t.Errorf("1xx %v, response %d %q, headers %v", statuses, resp.StatusCode, body, resp.Header)
	}
}

func TestResponseEncoderNativeHTTPBehavior(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer()
	s.Handle("GET /head", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		w.Header().Set("Content-Type", "text/plain")
		_, err := io.WriteString(w, "head body")
		return err
	}))
	s.Handle("GET /empty", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		w.WriteHeader(http.StatusNoContent)
		_, err := io.WriteString(w, "discarded body")
		if !errors.Is(err, http.ErrBodyNotAllowed) {
			t.Errorf("204 Write = %v, want ErrBodyNotAllowed", err)
		}
		return nil
	}))
	server := httptest.NewServer(s)
	defer server.Close()
	for _, tt := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"head", http.MethodHead, "/head", http.StatusOK},
		{"no content", http.MethodGet, "/empty", http.StatusNoContent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequestWithContext(t.Context(), tt.method, server.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := server.Client().Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tt.status || len(body) != 0 {
				t.Errorf("response %d %q", resp.StatusCode, body)
			}
		})
	}
}

func TestResponseEncoderNativeHijack(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	s := httpx.NewServer(httpx.WithLogger(testLogger(slog.New(slog.NewTextHandler(&logs, nil)))))
	finished := make(chan struct{})
	s.Handle("GET /", echo, nil, httpx.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := io.WriteString(buffered, "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nhijacked"); err != nil {
			return err
		}
		if err := buffered.Flush(); err != nil {
			return err
		}
		return errors.New("encoder failed after hijack")
	}))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		s.ServeHTTP(w, r)
	}))
	defer server.Close()
	resp, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	<-finished
	if resp.StatusCode != http.StatusOK || string(body) != "hijacked" {
		t.Errorf("response %d %q", resp.StatusCode, body)
	}
	if !strings.Contains(logs.String(), "encoder failed after hijack") || strings.Contains(logs.String(), "httpx: request failed") {
		t.Errorf("logs = %q", logs.String())
	}
}

func TestResponseEncoderMultipartFileLifetime(t *testing.T) {
	t.Parallel()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	file, err := form.CreateFormFile("File", "upload.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, "uploaded content"); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/upload", &body)
	r.Header.Set("Content-Type", form.FormDataContentType())
	// Force a disk-backed upload without allocating a 32 MiB body. The route
	// binder reuses the parsed form, and cleanup must wait for the encoder.
	if err := r.ParseMultipartForm(0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.MultipartForm.RemoveAll() }()
	s := httpx.NewServer()
	var header *multipart.FileHeader
	httpx.Route[userReq](s, "POST /upload", func(_ context.Context, req any) (any, error) {
		header = req.(*userReq).File
		return header, nil
	}, httpx.WithResponseEncoder(func(w http.ResponseWriter, req *http.Request, reply any) error {
		if req.MultipartForm == nil || reply != header {
			t.Errorf("encoder form %v, reply %v", req.MultipartForm, reply)
		}
		f, err := header.Open()
		if err != nil {
			return err
		}
		defer f.Close()
		if _, ok := f.(*os.File); !ok {
			t.Errorf("upload is not disk-backed: %T", f)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, err = io.Copy(w, f)
		return err
	}))
	w := do(s, r)
	if w.Code != http.StatusOK || w.Body.String() != "uploaded content" {
		t.Fatalf("response %d %q", w.Code, w.Body.String())
	}
	f, err := header.Open()
	if f != nil {
		f.Close()
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("upload after response: %v, want removed temporary file", err)
	}
}
