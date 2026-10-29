package archtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/url"
	"testing"

	brokermemory "github.com/nostosx/servex-contrib/broker/memory"
	cachememory "github.com/nostosx/servex-contrib/cache/memory"
	database "github.com/nostosx/servex-contrib/database/sql"
	discoverymemory "github.com/nostosx/servex-contrib/discovery/memory"
	jobqueuememory "github.com/nostosx/servex-contrib/jobqueue/memory"
	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
	"github.com/nostosx/servex/storage/cache"
	"github.com/nostosx/servex/transport"
)

// The foundation packages do not import each other; they meet through
// structural typing. These assertions pin the shapes they rely on.
var (
	_ tracing.Carrier = metadata.MD(nil)
	_ health.Checker  = (*app.App)(nil)
)

// Reference implementations satisfy the contracts they implement.
var (
	_ discovery.Registrar  = (*discoverymemory.Registry)(nil)
	_ discovery.Discoverer = (*discoverymemory.Registry)(nil)
	_ cache.Cache          = (*cachememory.Cache)(nil)
	_ broker.Publisher     = (*brokermemory.Broker)(nil)
	_ broker.Subscriber    = (*brokermemory.Broker)(nil)
	_ jobqueue.Enqueuer    = (*jobqueuememory.Queue)(nil)
	_ jobqueue.Runner      = (*jobqueuememory.Queue)(nil)

	// Both in-memory implementations opt in to start readiness.
	_ broker.ReadySubscriber = (*brokermemory.Broker)(nil)
	_ jobqueue.ReadyRunner   = (*jobqueuememory.Queue)(nil)
)

// The HTTP server is a transport.Server, an http.Handler, reports runtime
// failures through the optional Err method app watches, and can be handed
// to discovery.Registration.
var (
	_ transport.Server                = (*httpx.Server)(nil)
	_ http.Handler                    = (*httpx.Server)(nil)
	_ interface{ Err() <-chan error } = (*httpx.Server)(nil)
	_ interface{ URL() *url.URL }     = transport.Server(nil)
)

// TestOptionalHookMethods checks that the hooks returned by constructors
// implement the optional methods app discovers by type assertion.
func TestOptionalHookMethods(t *testing.T) {
	type errReporter interface{ Err() <-chan error }
	type checker interface {
		Check(ctx context.Context) error
	}
	type drainer interface {
		Drain(ctx context.Context) error
	}

	db := sql.OpenDB(nopConnector{})
	t.Cleanup(func() { db.Close() })
	if _, ok := database.Hook("db", db).(checker); !ok {
		t.Error("database.Hook does not implement Check")
	}
	if _, ok := database.Hook("db", db, database.WithReadiness(false)).(checker); ok {
		t.Error("database.Hook implements Check with readiness disabled")
	}
	consumer := broker.NewConsumer(brokermemory.New(), "t", "", nopEndpoint)
	if _, ok := consumer.(errReporter); !ok {
		t.Error("broker.NewConsumer does not implement Err")
	}
	if _, ok := consumer.(checker); !ok {
		t.Error("broker.NewConsumer does not implement Check")
	}
	worker := jobqueue.NewWorker(jobqueuememory.New(), nil)
	if _, ok := worker.(errReporter); !ok {
		t.Error("jobqueue.NewWorker does not implement Err")
	}
	if _, ok := worker.(checker); !ok {
		t.Error("jobqueue.NewWorker does not implement Check")
	}
	if _, ok := discovery.Registration(discoverymemory.New(), discovery.Instance{Name: "svc"}).(drainer); !ok {
		t.Error("discovery.Registration does not implement Drain")
	}
	if _, ok := app.Hook(httpx.NewServer()).(errReporter); !ok {
		t.Error("httpx.Server does not implement Err")
	}
}

func nopEndpoint(context.Context, any) (any, error) { return nil, nil }

// nopConnector is a driver.Connector that never connects; the test only
// inspects method sets.
type nopConnector struct{}

func (nopConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("archtest: no connection")
}

func (nopConnector) Driver() driver.Driver { return nil }
