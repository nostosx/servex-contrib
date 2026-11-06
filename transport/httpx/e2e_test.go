package httpx_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"
	httptransport "github.com/nostosx/servex/transport/http"
)

// errUserNotFound is the business error returned by the user service.
var errUserNotFound = errorx.New(errorx.NotFound, "USER_NOT_FOUND", "user not found")

type e2eGetUser struct {
	ID      int    `json:"id"`
	Verbose bool   `json:"verbose"`
	Name    string `json:"name,omitzero"`
}

type e2eUser struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Verbose   bool   `json:"verbose"`
	RequestID string `json:"request_id"`
}

func TestEndToEndThroughDiscovery(t *testing.T) {
	ctx := t.Context()
	reg := memory.New()

	// The App is created after the server it contains, so /readyz reaches
	// it through the variable assigned below, before anything is served.
	var a *app.App
	srv := httpx.NewServer(
		httpx.WithAddress("127.0.0.1:0"),
		httpx.WithHealth(health.CheckerFunc(func(ctx context.Context) error { return a.Check(ctx) })),
		httpx.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
	)
	httptransport.Route[e2eGetUser](srv, "GET /v1/users/{id}", func(ctx context.Context, req any) (any, error) {
		r := req.(*e2eGetUser)
		if r.ID == 404 {
			return nil, errUserNotFound.WithMetadata(map[string]string{"id": fmt.Sprint(r.ID)})
		}
		info, _ := metadata.FromContext(ctx)
		return &e2eUser{ID: r.ID, Name: "user", Verbose: r.Verbose, RequestID: info.Header.Get("x-request-id")}, nil
	}, httptransport.WithOperation("users.Get"))
	httptransport.Route[e2eUser](srv, "POST /v1/users/{id}", func(_ context.Context, req any) (any, error) {
		return req, nil
	})

	var server httptransport.Server = srv
	var router httptransport.Router = srv
	router.HandleHTTP("GET /raw", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	a = app.New(
		app.WithHooks(server, discovery.Registration(reg, discovery.Instance{Name: "user", Version: "v1"}, server)),
		app.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
	)
	if err := a.Start(ctx); err != nil {
		t.Fatalf("app start: %v", err)
	}
	t.Cleanup(func() {
		if err := a.Stop(context.Background()); err != nil {
			t.Errorf("app stop: %v", err)
		}
	})

	resp, err := http.Get(server.URL().String() + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("readyz = %d, want 200", resp.StatusCode)
	}
	resp, err = http.Get(server.URL().String() + "/raw")
	if err != nil {
		t.Fatalf("raw route: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("raw route = %d, want 204", resp.StatusCode)
	}

	requestID := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			info.Header.Set("x-request-id", "req-1")
			return next(ctx, req)
		}
	}
	client, err := httpx.NewClient(ctx, "discovery:///user",
		httpx.WithDiscoverer(reg), httpx.WithClientMiddleware(requestID))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	var caller httptransport.Client = client
	defer caller.Close()

	getUser := httptransport.Call[e2eGetUser, e2eUser](caller, "GET", "/v1/users/{id}")
	reply, err := getUser(ctx, &e2eGetUser{ID: 7, Verbose: true})
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if got := *reply; got != (e2eUser{ID: 7, Name: "user", Verbose: true, RequestID: "req-1"}) {
		t.Errorf("reply = %+v", got)
	}

	created, err := client.Endpoint("POST", "/v1/users/{id}", func() any { return new(e2eUser) })(ctx, &e2eUser{ID: 9, Name: "neo"})
	if err != nil {
		t.Fatalf("post user: %v", err)
	}
	if got := *created.(*e2eUser); got.ID != 9 || got.Name != "neo" {
		t.Errorf("created = %+v", got)
	}

	_, err = getUser(ctx, &e2eGetUser{ID: 404})
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok {
		t.Fatalf("err = %v, want *errorx.Error", err)
	}
	if !errors.Is(err, errUserNotFound) || e.Code != errorx.NotFound || e.Reason != "USER_NOT_FOUND" ||
		e.Message != "user not found" || e.Metadata["id"] != "404" {
		t.Errorf("err = %+v", e)
	}

	// Stopping the App deregisters the instance before the server closes.
	if err := a.Stop(ctx); err != nil {
		t.Fatalf("app stop: %v", err)
	}
	w, err := reg.Watch(ctx, "user")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(); err != nil {
			t.Errorf("watcher stop: %v", err)
		}
	}()
	if instances, err := w.Next(); err != nil || len(instances) != 0 {
		t.Errorf("instances after stop = %v, %v; want none", instances, err)
	}
}
