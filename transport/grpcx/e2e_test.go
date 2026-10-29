package grpcx_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

func TestEndToEndThroughDiscovery(t *testing.T) {
	ctx := t.Context()
	n := newNetwork()
	reg := memory.New()

	// The server middleware proves that endpoint middleware is shared with
	// other transports: it reads only metadata.Info.
	var serverOps []string
	audit := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			serverOps = append(serverOps, info.Operation)
			return next(ctx, req)
		}
	}

	// The App is created after the server it contains, so health checks
	// reach it through the variable assigned below, before anything is served.
	var a *app.App
	svc := &testService{id: "user-1", blocked: make(chan struct{}, 1)}
	srv := grpcx.NewServer(
		grpcx.WithListener(n.listen("127.0.0.1:9300")),
		grpcx.WithMiddleware(audit),
		grpcx.WithHealth(health.CheckerFunc(func(ctx context.Context) error { return a.Check(ctx) })),
	)
	testpb.RegisterTestServiceServer(srv, svc)
	a = app.New(
		app.WithHooks(srv, discovery.Registration(reg, discovery.Instance{Name: "user", Version: "v1"}, srv)),
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

	user := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			info.Header.Set("x-user", "neo")
			return next(ctx, req)
		}
	}
	client, err := grpcx.NewClient(ctx, "discovery:///user",
		grpcx.WithDiscoverer(reg), grpcx.WithClientMiddleware(user), grpcx.WithDialOptions(n.dialOption()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	hc := grpc_health_v1.NewHealthClient(client.Conn())
	if resp, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil || resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("health = %v, %v", resp, err)
	}

	tc := testpb.NewTestServiceClient(client.Conn())
	resp, err := tc.UnaryCall(ctx, simple("ping"))
	if err != nil {
		t.Fatalf("UnaryCall: %v", err)
	}
	if resp.GetServerId() != "user-1" || resp.GetUsername() != "neo" || string(resp.GetPayload().GetBody()) != "ping" {
		t.Errorf("resp = %v", resp)
	}

	_, err = tc.UnaryCall(ctx, simple("notfound"))
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || !errors.Is(err, errUserNotFound) || e.Message != "user not found" || e.Metadata["id"] != "404" {
		t.Errorf("err = %v", err)
	}
	if want := "/grpc.testing.TestService/UnaryCall"; len(serverOps) != 2 || serverOps[0] != want || serverOps[1] != want {
		t.Errorf("server ops = %v (health probes must bypass middleware)", serverOps)
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
