package archtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/url"
	"testing"

	discoverymemory "github.com/nostosx/servex-contrib/discovery/memory"
	jobqueuememory "github.com/nostosx/servex-contrib/job/memory"
	brokermemory "github.com/nostosx/servex-contrib/messaging/memory"
	cachememory "github.com/nostosx/servex-contrib/storage/cache/memory"
	database "github.com/nostosx/servex-contrib/storage/database/sql"
	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/messaging"
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
	_ messaging.Publisher  = (*brokermemory.Broker)(nil)
	_ messaging.Subscriber = (*brokermemory.Broker)(nil)
	_ job.Enqueuer         = (*jobqueuememory.Queue)(nil)
	_ job.Runner           = (*jobqueuememory.Queue)(nil)

	// Both in-memory implementations opt in to start readiness.
	_ messaging.ReadySubscriber = (*brokermemory.Broker)(nil)
	_ job.ReadyRunner           = (*jobqueuememory.Queue)(nil)
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
	consumer := messaging.NewConsumer(brokermemory.New(), "t", "", nopHandler)
	if _, ok := consumer.(errReporter); !ok {
		t.Error("messaging.NewConsumer does not implement Err")
	}
	if _, ok := consumer.(checker); !ok {
		t.Error("messaging.NewConsumer does not implement Check")
	}
	worker := job.NewWorker(jobqueuememory.New(), nil)
	if _, ok := worker.(errReporter); !ok {
		t.Error("job.NewWorker does not implement Err")
	}
	if _, ok := worker.(checker); !ok {
		t.Error("job.NewWorker does not implement Check")
	}
	if _, ok := discovery.Registration(discoverymemory.New(), discovery.Instance{Name: "svc"}).(drainer); !ok {
		t.Error("discovery.Registration does not implement Drain")
	}
	if _, ok := app.Hook(httpx.NewServer()).(errReporter); !ok {
		t.Error("httpx.Server does not implement Err")
	}
}

func nopHandler(context.Context, *messaging.Message) error { return nil }

// nopConnector is a driver.Connector that never connects; the test only
// inspects method sets.
type nopConnector struct{}

func (nopConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("archtest: no connection")
}

func (nopConnector) Driver() driver.Driver { return nil }
