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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	httptransport "github.com/nostosx/servex/transport/http"
)

func TestRequestDecoderReplacesBindingAndPreservesOrder(t *testing.T) {
	t.Parallel()
	var order []string
	s := httpx.NewServer(
		httpx.WithHTTPMiddleware(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "http before")
				next.ServeHTTP(w, r)
				order = append(order, "http after")
			})
		}),
		httpx.WithMiddleware(func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				order = append(order, "endpoint before")
				if req.(*userReq).ID != "custom" {
					t.Fatal("default binding overwrote the decoded request")
				}
				reply, err := next(ctx, req)
				order = append(order, "endpoint after")
				return reply, err
			}
		}),
	)
	var decoderContext context.Context
	httptransport.RouteFunc(s, "POST /items/{id}", func(ctx context.Context, req *userReq) (string, error) {
		order = append(order, "handler")
		if ctx != decoderContext || req.ID != "custom" || req.Name != "opaque body" {
			t.Fatal("decoded request or context changed")
		}
		return req.ID, nil
	}, httptransport.WithOperation("items.Create"), httptransport.WithRequestDecoder(func(r *http.Request, dst any) error {
		order = append(order, "decoder")
		decoderContext = r.Context()
		info, ok := metadata.FromContext(r.Context())
		if !ok || info.Kind != metadata.KindServer || info.Protocol != "http" || info.Operation != "items.Create" || info.Header.Get("x-item") != "custom" {
			t.Fatalf("decoder metadata = %+v (present %v)", info, ok)
		}
		if r.PathValue("id") != "path" || r.URL.Query().Get("id") != "query" {
			t.Fatal("decoder lost HTTP parameters")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		dst.(*userReq).ID = r.Header.Get("X-Item")
		dst.(*userReq).Name = string(body)
		return nil
	}), httptransport.WithResponseEncoder(func(w http.ResponseWriter, r *http.Request, reply any) error {
		order = append(order, "encoder")
		if r.Context() != decoderContext {
			t.Fatal("encoder lost decoder metadata context")
		}
		_, err := io.WriteString(w, reply.(string))
		return err
	}))
	r := httptest.NewRequest(http.MethodPost, "/items/path?id=query", strings.NewReader("opaque body"))
	r.Header.Set("Content-Type", "application/custom")
	r.Header.Set("X-Item", "custom")
	w := do(s, r)
	if w.Code != http.StatusOK || w.Body.String() != "custom" {
		t.Fatalf("response = %d %q", w.Code, w.Body.String())
	}
	want := []string{"http before", "decoder", "endpoint before", "handler", "endpoint after", "encoder", "http after"}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestRequestDecoderNilRestoresDefaultBinding(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer()
	httptransport.Route[userReq](s, "POST /items/{id}", echo,
		httptransport.WithRequestDecoder(func(*http.Request, any) error {
			t.Fatal("replaced decoder ran")
			return nil
		}), httptransport.WithRequestDecoder(nil))
	r := httptest.NewRequest(http.MethodPost, "/items/path?name=query", strings.NewReader(`{"id":"body","name":"body","age":7}`))
	r.Header.Set("Content-Type", "application/json")
	w := do(s, r)
	got := decodeJSON[userReq](t, w)
	if w.Code != http.StatusOK || got.ID != "path" || got.Name != "query" || got.Age != 7 {
		t.Fatalf("default binding = %d %+v", w.Code, got)
	}
}

func TestRequestDecoderErrorsSkipEndpointAndEncoder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status int
		reason string
	}{
		{"plain", errors.New("private request payload"), http.StatusBadRequest, "DECODE_REQUEST"},
		{"classified", errorx.New(errorx.PermissionDenied, "DENIED", "access denied"), http.StatusForbidden, "DENIED"},
		{"wrapped classified", fmt.Errorf("decoder: %w", errorx.New(errorx.InvalidArgument, "BAD_FIELD", "invalid field")), http.StatusBadRequest, "BAD_FIELD"},
		{"body overflow", fmt.Errorf("decoder: %w", &http.MaxBytesError{Limit: 1}), http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE"},
		{"canceled", fmt.Errorf("decoder: %w", context.Canceled), 499, ""},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httpx.NewServer(httpx.WithMiddleware(func(next endpoint.Endpoint) endpoint.Endpoint {
				return func(context.Context, any) (any, error) {
					t.Fatal("endpoint middleware ran after decoder failure")
					return nil, nil
				}
			}))
			httptransport.Route[userReq](s, "POST /", func(context.Context, any) (any, error) {
				t.Fatal("business handler ran after decoder failure")
				return nil, nil
			}, httptransport.WithRequestDecoder(func(*http.Request, any) error { return tc.err }),
				httptransport.WithResponseEncoder(func(http.ResponseWriter, *http.Request, any) error {
					t.Fatal("success encoder ran after decoder failure")
					return nil
				}))
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("Accept", "application/yaml")
			w := do(s, r)
			got := decodeJSON[errBody](t, w)
			if w.Code != tc.status || got.Reason != tc.reason || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("response = %d %+v, type %q", w.Code, got, w.Header().Get("Content-Type"))
			}
			if strings.Contains(w.Body.String(), "private request payload") {
				t.Fatal("unclassified decoder error exposed request data")
			}
		})
	}
}

func TestRequestDecoderCannotBypassBodyLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		declared      bool
		readBytes     int
		wantDecodings int
	}{
		{"declared overflow", true, 0, 0},
		{"stream overflow", false, 16, 1},
		{"unread remainder", false, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decodings := 0
			s := httpx.NewServer(httpx.WithMaxBodyBytes(4))
			httptransport.Route[userReq](s, "POST /", func(context.Context, any) (any, error) {
				t.Fatal("oversized request reached business")
				return nil, nil
			}, httptransport.WithRequestDecoder(func(r *http.Request, _ any) error {
				decodings++
				_, err := io.CopyN(io.Discard, r.Body, int64(tc.readBytes))
				return err
			}))
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("oversized"))
			if !tc.declared {
				r.ContentLength = -1
			}
			assertBodyLimitError(t, do(s, r))
			if decodings != tc.wantDecodings {
				t.Fatalf("decoder called %d times, want %d", decodings, tc.wantDecodings)
			}
		})
	}
}

func TestRequestDecoderNilFactory(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer()
	decoded := false
	s.Handle("GET /", func(_ context.Context, req any) (any, error) {
		if !decoded || req != nil {
			t.Fatal("nil factory changed decoder or endpoint behavior")
		}
		return nil, nil
	}, nil, httptransport.WithRequestDecoder(func(_ *http.Request, dst any) error {
		decoded = true
		if dst != nil {
			t.Fatal("nil factory produced a non-nil target")
		}
		return nil
	}))
	if w := do(s, httptest.NewRequest(http.MethodGet, "/", nil)); w.Code != http.StatusNoContent {
		t.Fatalf("response = %d %q", w.Code, w.Body.String())
	}
}

func TestRequestDecoderOwnsConcurrentTargets(t *testing.T) {
	t.Parallel()
	s := httpx.NewServer()
	httptransport.Route[userReq](s, "POST /", echo, httptransport.WithRequestDecoder(func(r *http.Request, dst any) error {
		req := dst.(*userReq)
		if req.ID != "" {
			t.Error("request target was reused")
		}
		req.ID = r.Header.Get("X-Item")
		return nil
	}))
	var wg sync.WaitGroup
	type response struct {
		index int
		body  *httptest.ResponseRecorder
	}
	responses := make(chan response, 16)
	for i := range 16 {
		wg.Go(func() {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("X-Item", fmt.Sprint(i))
			responses <- response{index: i, body: do(s, r)}
		})
	}
	wg.Wait()
	close(responses)
	for response := range responses {
		w := response.body
		if got := decodeJSON[userReq](t, w); w.Code != http.StatusOK || got.ID != fmt.Sprint(response.index) {
			t.Errorf("response = %d %+v", w.Code, got)
		}
	}
}

func TestRequestDecoderMultipartCleanup(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%v", fail), func(t *testing.T) {
			var body bytes.Buffer
			form := multipart.NewWriter(&body)
			part, err := form.CreateFormFile("file", "upload.txt")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(part, "upload contents"); err != nil {
				t.Fatal(err)
			}
			if err := form.Close(); err != nil {
				t.Fatal(err)
			}
			var header *multipart.FileHeader
			s := httpx.NewServer()
			httptransport.Route[userReq](s, "POST /", func(_ context.Context, req any) (any, error) {
				if fail || req.(*userReq).File != header {
					t.Fatal("handler received an invalid upload")
				}
				return header, nil
			}, httptransport.WithRequestDecoder(func(r *http.Request, dst any) error {
				if err := r.ParseMultipartForm(0); err != nil {
					return err
				}
				// Register fallback cleanup so a regression cannot leak the fixture.
				t.Cleanup(func() { _ = r.MultipartForm.RemoveAll() })
				header = r.MultipartForm.File["file"][0]
				dst.(*userReq).File = header
				if fail {
					return errors.New("bad multipart request")
				}
				return nil
			}), httptransport.WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, reply any) error {
				f, err := reply.(*multipart.FileHeader).Open()
				if err != nil {
					return err
				}
				defer f.Close()
				if _, ok := f.(*os.File); !ok {
					t.Error("fixture did not create a disk-backed upload")
				}
				_, err = io.Copy(w, f)
				return err
			}))
			r := httptest.NewRequest(http.MethodPost, "/", &body)
			r.Header.Set("Content-Type", form.FormDataContentType())
			w := do(s, r)
			if fail {
				if w.Code != http.StatusBadRequest {
					t.Fatalf("response = %d %q", w.Code, w.Body.String())
				}
			} else if w.Code != http.StatusOK || w.Body.String() != "upload contents" {
				t.Fatalf("response = %d %q", w.Code, w.Body.String())
			}
			if header == nil {
				t.Fatal("decoder did not parse the multipart body")
			}
			f, err := header.Open()
			if f != nil {
				_ = f.Close()
			}
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("upload after response = %v; want removed", err)
			}
		})
	}
}
