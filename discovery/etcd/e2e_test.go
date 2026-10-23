package etcd

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"
	"github.com/nostosx/servex/transport/httpx"
)

// runEndToEnd serves an HTTP endpoint from an App that registers itself in
// reg, calls it through transport.NewClient("discovery:///greeter") backed
// by dis, and checks that stopping the App removes the node.
func runEndToEnd(t *testing.T, reg discovery.Registrar, dis discovery.Discoverer) {
	ctx := t.Context()
	srv := httpx.NewServer(httpx.Address("127.0.0.1:0"), httpx.Logger(slog.New(slog.DiscardHandler)))
	srv.HandleHTTP("GET /hello", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello") // a failed write surfaces as a client error
	}))
	a := app.New(
		app.WithHooks(srv, discovery.Registration(reg, discovery.Instance{Name: "greeter", Version: "v1"}, srv)),
		app.WithLogger(slog.New(slog.DiscardHandler)),
	)
	if err := a.Start(ctx); err != nil {
		t.Fatalf("app start: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if err := a.Stop(context.Background()); err != nil {
				t.Errorf("app stop: %v", err)
			}
		}
	})

	client, err := transport.NewClient(ctx, "discovery:///greeter",
		transport.WithDiscoverer(dis), transport.WithScheme("http"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	hello := client.Endpoint("hello", transport.InvokerFunc(func(ctx context.Context, addr string, _ any) (any, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/hello", nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, errorx.New(errorx.Unavailable, "CALL_FAILED", "call failed").WithCause(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}))

	// The registration may take a moment to reach the watcher.
	reply := eventually(t, func() (any, error) { return hello(ctx, nil) })
	if reply != "hello" {
		t.Fatalf("reply = %v, want hello", reply)
	}

	if err := a.Stop(ctx); err != nil {
		t.Fatalf("app stop: %v", err)
	}
	stopped = true
	deadline := time.Now().Add(patience)
	for {
		_, err := hello(ctx, nil)
		if errors.Is(err, selector.ErrNoAvailable) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("call after Stop error = %v, want selector.ErrNoAvailable", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// eventually retries call until it succeeds.
func eventually(t *testing.T, call func() (any, error)) any {
	t.Helper()
	deadline := time.Now().Add(patience)
	for {
		reply, err := call()
		if err == nil {
			return reply
		}
		if time.Now().After(deadline) {
			t.Fatalf("call error = %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
