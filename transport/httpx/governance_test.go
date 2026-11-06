package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	client "github.com/nostosx/servex-contrib/transport/client"
	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport"
	httptransport "github.com/nostosx/servex/transport/http"
)

func TestOperationPolicyRetriesActualHTTPAttempts(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":14,"message":"unavailable"}`))
			return
		}
		_, _ = w.Write([]byte(`{"name":"book"}`))
	}))
	defer server.Close()
	attempts := 0
	c, err := httpx.NewClient(t.Context(), server.URL, httpx.WithHTTPClient(server.Client()), httpx.WithOperationPolicies(map[string]client.Policy{"GET /catalog": {MaxAttempts: 2, RetrySafe: true, Timeout: time.Second, RetryIf: func(err error) bool { return errorx.CodeOf(err) == errorx.Unavailable }}}), httpx.WithClientAttemptMiddleware(func(next transport.Invoker) transport.Invoker {
		return transport.InvokerFunc(func(ctx context.Context, address string, request any) (any, error) {
			attempts++
			info, ok := transport.AttemptFromContext(ctx)
			if !ok || info.Number != attempts {
				t.Fatalf("attempt=%+v", info)
			}
			return next.Invoke(ctx, address, request)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	type product struct {
		Name string `json:"name"`
	}
	result, err := httptransport.Call[struct{}, product](c, http.MethodGet, "/catalog")(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Name != "book" || attempts != 2 || requests.Load() != 2 {
		t.Fatalf("result=%v attempts=%d requests=%d", result, attempts, requests.Load())
	}
}
