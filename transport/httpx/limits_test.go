package httpx_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
)

type limitedRequest struct {
	Name string                `json:"name"`
	File *multipart.FileHeader `json:"file"`
}

func multipartLimitBody(t *testing.T, fileSize int64) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("name", "alice"); err != nil {
		t.Fatal(err)
	}
	part, err := mw.CreateFormFile("file", "upload.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(part, repeatingLimitReader{}, fileSize); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), mw.FormDataContentType()
}

type repeatingLimitReader struct{}

func (repeatingLimitReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func assertBodyLimitError(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body = %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	body := decodeJSON[errBody](t, w)
	if body.Code != "InvalidArgument" || body.Reason != "BODY_TOO_LARGE" || body.Message != "request body too large" {
		t.Errorf("error body = %+v", body)
	}
	if body.Metadata == nil || len(body.Metadata) != 0 {
		t.Errorf("metadata = %#v, want empty object", body.Metadata)
	}
}

func TestMaxBodyBytesBoundaries(t *testing.T) {
	multipartBody, multipartType := multipartLimitBody(t, 12)
	for _, format := range []struct {
		name        string
		contentType string
		body        []byte
	}{
		{name: "json", contentType: "application/json", body: []byte(`{"name":"alice"}`)},
		{name: "form", contentType: "application/x-www-form-urlencoded", body: []byte("name=alice")},
		{name: "multipart", contentType: multipartType, body: multipartBody},
	} {
		for _, unknownLength := range []bool{false, true} {
			for _, nilRequest := range []bool{false, true} {
				for _, exceeds := range []bool{false, true} {
					name := fmt.Sprintf("%s/unknown=%t/nil-request=%t/exceeds=%t", format.name, unknownLength, nilRequest, exceeds)
					t.Run(name, func(t *testing.T) {
						limit := int64(len(format.body))
						if exceeds {
							limit--
						}
						middlewareCalled, endpointCalled := false, false
						s := httpx.NewServer(httpx.WithMaxBodyBytes(limit), httpx.WithMiddleware(func(next endpoint.Endpoint) endpoint.Endpoint {
							return func(ctx context.Context, req any) (any, error) {
								middlewareCalled = true
								return next(ctx, req)
							}
						}))
						ep := func(_ context.Context, req any) (any, error) {
							endpointCalled = true
							if nilRequest {
								if req != nil {
									t.Errorf("request = %v, want nil", req)
								}
							} else if got := req.(*limitedRequest); got.Name != "alice" {
								t.Errorf("bound name = %q", got.Name)
							}
							return nil, nil
						}
						if nilRequest {
							s.Handle("POST /", ep, nil)
						} else {
							httpx.Route[limitedRequest](s, "POST /", ep)
						}
						r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(format.body))
						r.Header.Set("Content-Type", format.contentType)
						if unknownLength {
							r.ContentLength = -1
						}
						w := do(s, r)
						if exceeds {
							assertBodyLimitError(t, w)
							if middlewareCalled || endpointCalled {
								t.Errorf("oversized request called middleware=%t endpoint=%t", middlewareCalled, endpointCalled)
							}
						} else if w.Code != http.StatusNoContent || !middlewareCalled || !endpointCalled {
							t.Errorf("exact boundary: status=%d middleware=%t endpoint=%t body=%s", w.Code, middlewareCalled, endpointCalled, w.Body)
						}
					})
				}
			}
		}
	}
}

func TestMaxBodyBytesMultipartEpilogue(t *testing.T) {
	body, contentType := multipartLimitBody(t, 4)
	// A multipart parser may finish before consuming the epilogue. Make it
	// longer than the parser's read buffer to exercise the remaining-body check.
	body = append(body, strings.Repeat("epilogue", 2048)...)
	for _, exceeds := range []bool{false, true} {
		t.Run(fmt.Sprintf("exceeds=%t", exceeds), func(t *testing.T) {
			limit := int64(len(body))
			if exceeds {
				limit--
			}
			called := false
			s := httpx.NewServer(httpx.WithMaxBodyBytes(limit))
			httpx.Route[limitedRequest](s, "POST /", func(context.Context, any) (any, error) {
				called = true
				return nil, nil
			})
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			r.ContentLength = -1
			r.Header.Set("Content-Type", contentType)
			w := do(s, r)
			if exceeds {
				assertBodyLimitError(t, w)
				if called {
					t.Error("oversized epilogue reached endpoint")
				}
			} else if w.Code != http.StatusNoContent || !called {
				t.Errorf("exact epilogue boundary: status=%d called=%t body=%s", w.Code, called, w.Body)
			}
		})
	}
}

type observedLimitBody struct {
	reads int
}

func (b *observedLimitBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("body must not be read")
}
func (*observedLimitBody) Close() error { return nil }

func TestMaxBodyBytesRejectsDeclaredLengthBeforeBinding(t *testing.T) {
	s := httpx.NewServer(httpx.WithMaxBodyBytes(8))
	s.Handle("POST /", func(context.Context, any) (any, error) {
		t.Error("endpoint called")
		return nil, nil
	}, func() any {
		t.Error("request factory called")
		return new(limitedRequest)
	})
	body := new(observedLimitBody)
	r := httptest.NewRequest(http.MethodPost, "/", body)
	r.ContentLength = 9
	assertBodyLimitError(t, do(s, r))
	if body.reads != 0 {
		t.Errorf("body reads = %d, want zero", body.reads)
	}
}

func TestMaxBodyBytesBindingFailureDoesNotDrain(t *testing.T) {
	s := httpx.NewServer(httpx.WithMaxBodyBytes(8))
	httpx.Route[struct{ Count int }](s, "POST /", func(context.Context, any) (any, error) {
		t.Error("endpoint called")
		return nil, nil
	})
	body := new(observedLimitBody)
	r := httptest.NewRequest(http.MethodPost, "/", body)
	r.Header.Set("Content-Type", "invalid content type;")
	w := do(s, r)
	if w.Code != http.StatusBadRequest || decodeJSON[errBody](t, w).Reason != "INVALID_CONTENT_TYPE" {
		t.Errorf("binding error: status=%d body=%s", w.Code, w.Body)
	}
	if body.reads != 0 {
		t.Errorf("body reads = %d after failed binding, want zero", body.reads)
	}
}

func TestDefaultMaxBodyBytes(t *testing.T) {
	s := httpx.NewServer()
	s.Handle("POST /", func(context.Context, any) (any, error) { return nil, nil }, nil)
	for _, tc := range []struct {
		name   string
		size   int64
		hidden bool // hide the length so the limit applies while reading
		status int
	}{
		{name: "at limit", size: httpx.DefaultMaxBodyBytes, status: http.StatusNoContent},
		{name: "declared over limit", size: httpx.DefaultMaxBodyBytes + 1, status: http.StatusRequestEntityTooLarge},
		{name: "streamed over limit", size: httpx.DefaultMaxBodyBytes + 1, hidden: true, status: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.hidden {
				body = io.LimitReader(repeatingLimitReader{}, tc.size)
			} else {
				body = bytes.NewReader(bytes.Repeat([]byte{'x'}, int(tc.size)))
			}
			w := do(s, httptest.NewRequest(http.MethodPost, "/", body))
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d; body = %s", w.Code, tc.status, w.Body)
			}
			if tc.status == http.StatusRequestEntityTooLarge {
				assertBodyLimitError(t, w)
			}
		})
	}
}

func TestMaxBodyBytesDisabledAndRawHandlers(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []httpx.ServerOption
	}{
		{name: "zero", opts: []httpx.ServerOption{httpx.WithMaxBodyBytes(0)}},
		{name: "negative", opts: []httpx.ServerOption{httpx.WithMaxBodyBytes(-1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httpx.NewServer(tc.opts...)
			httpx.Route[limitedRequest](s, "POST /bound", echo)
			s.Handle("POST /nil", func(context.Context, any) (any, error) { return nil, nil }, nil)
			r := httptest.NewRequest(http.MethodPost, "/bound", strings.NewReader(`{"name":"alice"}`))
			if w := do(s, r); w.Code != http.StatusOK {
				t.Errorf("disabled limit: status=%d body=%s", w.Code, w.Body)
			}
			body := new(observedLimitBody)
			if w := do(s, httptest.NewRequest(http.MethodPost, "/nil", body)); w.Code != http.StatusNoContent || body.reads != 0 {
				t.Errorf("disabled nil request: status=%d reads=%d", w.Code, body.reads)
			}
		})
	}
	t.Run("raw handler", func(t *testing.T) {
		s := httpx.NewServer(httpx.WithMaxBodyBytes(1))
		const payload = "a raw handler controls its body"
		s.HandleHTTP("POST /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != payload {
				t.Errorf("raw body=%q error=%v", body, err)
			}
			w.WriteHeader(http.StatusAccepted)
		}))
		for _, length := range []int64{-1, int64(len(payload))} {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
			r.ContentLength = length
			if w := do(s, r); w.Code != http.StatusAccepted {
				t.Errorf("raw status=%d body=%s", w.Code, w.Body)
			}
		}
	})
}

func TestMaxBodyBytesClientErrorSemantics(t *testing.T) {
	s := httpx.NewServer(httpx.WithMaxBodyBytes(2))
	httpx.Route[limitedRequest](s, "POST /limited", echo)
	s.Handle("GET /invalid", func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.InvalidArgument, "BODY_TOO_LARGE", "business validation")
	}, nil)
	s.Handle("GET /quota", func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.ResourceExhausted, "QUOTA", "quota exhausted")
	}, nil)
	host := httptest.NewTestServer(t, s)
	defer host.Close()
	httpClient := host.Client()
	client, err := httpx.NewClient(t.Context(), host.URL, httpx.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = httpx.Call[limitedRequest](client, "POST", "/limited")(t.Context(), &limitedRequest{Name: "alice"})
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.InvalidArgument || e.Reason != "BODY_TOO_LARGE" || errorx.IsRetryable(err) {
		t.Fatalf("limit client error = %v, want nonretryable InvalidArgument/BODY_TOO_LARGE", err)
	}
	for _, tc := range []struct {
		name      string
		path      string
		status    int
		code      errorx.Code
		retryable bool
	}{
		{name: "business invalid argument", path: "/invalid", status: 400, code: errorx.InvalidArgument},
		{name: "resource exhausted", path: "/quota", status: 429, code: errorx.ResourceExhausted, retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(s, httptest.NewRequest(http.MethodGet, tc.path, nil)); w.Code != tc.status {
				t.Errorf("status=%d, want %d; body=%s", w.Code, tc.status, w.Body)
			}
			_, err := httpx.Call[limitedRequest](client, "GET", tc.path)(t.Context(), nil)
			if errorx.CodeOf(err) != tc.code || errorx.IsRetryable(err) != tc.retryable {
				t.Errorf("client error=%v retryable=%t", err, errorx.IsRetryable(err))
			}
		})
	}
}

func TestMultipartTemporaryFilesRemovedAfterResponse(t *testing.T) {
	body, contentType := multipartLimitBody(t, 32<<20+1)
	for _, tc := range []struct {
		name     string
		endpoint endpoint.Endpoint
		encoder  bool
		status   int
	}{
		{name: "default", status: http.StatusNoContent},
		{name: "encoder", encoder: true, status: http.StatusAccepted},
		{name: "endpoint error", endpoint: func(context.Context, any) (any, error) {
			return nil, errorx.New(errorx.InvalidArgument, "REJECTED", "upload rejected")
		}, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httpx.NewServer(httpx.WithMaxBodyBytes(int64(len(body))))
			var filename string
			var form *multipart.Form
			var opts []httpx.RouteOption
			if tc.encoder {
				opts = append(opts, httpx.WithResponseEncoder(func(w http.ResponseWriter, r *http.Request, _ any) error {
					form = r.MultipartForm
					if _, err := os.Stat(filename); err != nil {
						t.Errorf("temporary file removed before encoder: %v", err)
					}
					w.WriteHeader(http.StatusAccepted)
					return nil
				}))
			}
			httpx.Route[limitedRequest](s, "POST /", func(ctx context.Context, req any) (any, error) {
				file, err := req.(*limitedRequest).File.Open()
				if err != nil {
					t.Fatal(err)
				}
				disk, ok := file.(*os.File)
				if !ok {
					t.Fatal("upload did not spill to a temporary file")
				}
				filename = disk.Name()
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				if tc.endpoint != nil {
					return tc.endpoint(ctx, req)
				}
				return nil, nil
			}, opts...)
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			r.Header.Set("Content-Type", contentType)
			w := do(s, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d, want %d; body=%s", w.Code, tc.status, w.Body)
			}
			if filename == "" {
				t.Fatal("endpoint did not open temporary file")
			}
			if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("temporary file remains after response: %v", err)
			}
			if tc.encoder && form == nil {
				t.Error("encoder did not receive multipart form")
			}
		})
	}
}
