package httpx

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestEndpointFilterExplicitOrderAndRequestMetadata(t *testing.T) {
	t.Parallel()
	var order []string
	observe := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request any) (any, error) {
				info, ok := metadata.FromContext(ctx)
				if !ok || info.Kind != metadata.KindServer || info.Protocol != "http" || info.Operation != "browser" || info.Header.Get("x-request") != "original" {
					t.Fatalf("missing request metadata: %+v", info)
				}
				order = append(order, name+".before")
				result, err := next(ctx, request)
				if errorx.CodeOf(err) != errorx.PermissionDenied {
					t.Fatalf("status was not classified: %v", err)
				}
				order = append(order, name+".after")
				return result, err
			}
		}
	}
	policies := []endpoint.Middleware{observe("outer"), nil, observe("inner")}
	filter := EndpointFilter("browser", policies...)
	policies[0] = nil
	handler := filter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info, _ := metadata.FromContext(r.Context())
		info.Header.Set("x-request", "changed")
		order = append(order, "handler")
		w.WriteHeader(http.StatusForbidden)
	}))
	r := httptest.NewRequest(http.MethodGet, "https://example.com/private?token=secret", nil)
	r.Header.Set("X-Request", "original")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	want := []string{"outer.before", "inner.before", "handler", "inner.after", "outer.after"}
	if w.Code != http.StatusForbidden || !slices.Equal(order, want) || r.Header.Get("X-Request") != "original" {
		t.Fatalf("status=%d order=%v request=%v", w.Code, order, r.Header)
	}
}

func TestEndpointFilterObservesNativeHTTPFailures(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		code errorx.Code
		want int
		run  http.Handler
		req  *http.Request
	}{
		{"routing", errorx.NotFound, http.StatusNotFound, http.NewServeMux(), httptest.NewRequest(http.MethodGet, "/missing", nil)},
		{"persistence", errorx.Internal, http.StatusInternalServerError, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "session persistence failed", http.StatusInternalServerError)
		}), httptest.NewRequest(http.MethodGet, "/session", nil)},
		{"csrf", errorx.PermissionDenied, http.StatusForbidden, http.NewCrossOriginProtection().Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})), func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "https://api.example/resource", nil)
			r.Header.Set("Origin", "https://attacker.example")
			return r
		}()},
		{"preflight", errorx.OK, http.StatusNoContent, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}), httptest.NewRequest(http.MethodOptions, "/resource", nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			observe := func(next endpoint.Endpoint) endpoint.Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					called = true
					result, err := next(ctx, req)
					if errorx.CodeOf(err) != tt.code {
						t.Errorf("code=%v, want %v", errorx.CodeOf(err), tt.code)
					}
					return result, err
				}
			}
			w := httptest.NewRecorder()
			EndpointFilter("http.request", observe)(tt.run).ServeHTTP(w, tt.req)
			if !called || w.Code != tt.want {
				t.Fatalf("observed=%v status=%d", called, w.Code)
			}
		})
	}
}

func TestEndpointFilterPanicAndRejectionAreExplicit(t *testing.T) {
	t.Parallel()
	marker := errors.New("private panic")
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(marker) })
	identity := func(next endpoint.Endpoint) endpoint.Endpoint { return next }
	func() {
		defer func() {
			if got := recover(); got != marker {
				t.Fatalf("panic changed: %v", got)
			}
		}()
		EndpointFilter("panic", identity)(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	recoverPanic := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (result any, err error) {
			defer func() {
				if recover() != nil {
					err = errorx.New(errorx.Internal, "PANIC", "request failed")
				}
			}()
			return next(ctx, request)
		}
	}
	w := httptest.NewRecorder()
	EndpointFilter("panic", recoverPanic)(handler).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "private") {
		t.Fatalf("explicit recovery status=%d body=%s", w.Code, w.Body)
	}
	for _, code := range []errorx.Code{errorx.Unauthenticated, errorx.ResourceExhausted, errorx.Internal} {
		deny := func(endpoint.Endpoint) endpoint.Endpoint {
			return func(context.Context, any) (any, error) { return nil, errorx.New(code, "DENIED", "private detail") }
		}
		w := httptest.NewRecorder()
		EndpointFilter("", deny)(handler).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != statusOf(code) || strings.Contains(w.Body.String(), "private") {
			t.Fatalf("code=%v status=%d body=%s", code, w.Code, w.Body)
		}
	}
	mux := http.NewServeMux()
	if EndpointFilter("", nil)(mux) != mux {
		t.Fatal("empty chain replaced the native handler")
	}
}

func TestEndpointFilterPreservesResponseController(t *testing.T) {
	t.Parallel()
	identity := func(next endpoint.Endpoint) endpoint.Endpoint { return next }
	filter := EndpointFilter("events", identity)
	w := httptest.NewRecorder()
	filter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Fatal(err)
		}
	})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events", nil))
	if !w.Flushed {
		t.Fatal("flush capability lost")
	}
	plain := &responsePlainWriter{httptest.NewRecorder()}
	filter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, ok := w.(http.Flusher); ok {
			t.Error("plain response writer gained flushing")
		}
		if err := http.NewResponseController(w).Flush(); !errors.Is(err, http.ErrNotSupported) {
			t.Errorf("unsupported flush=%v", err)
		}
	})).ServeHTTP(plain, httptest.NewRequest(http.MethodGet, "/plain", nil))
	control := &responseControlWriter{ResponseWriter: httptest.NewRecorder()}
	deadline := time.Now().Add(time.Second)
	filter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := http.NewResponseController(w).SetWriteDeadline(deadline); err != nil {
			t.Fatal(err)
		}
	})).ServeHTTP(control, httptest.NewRequest(http.MethodGet, "/deadline", nil))
	if control.writeDeadline != deadline {
		t.Fatal("write-deadline capability lost")
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	hijacker := &responseHijackWriter{ResponseWriter: httptest.NewRecorder(), conn: conn}
	filter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		got, _, err := http.NewResponseController(w).Hijack()
		if err != nil || got != conn {
			t.Fatalf("hijack=%v %v", got, err)
		}
	})).ServeHTTP(hijacker, httptest.NewRequest(http.MethodGet, "/upgrade", nil))
	if !hijacker.called {
		t.Fatal("hijacking capability lost")
	}
}
