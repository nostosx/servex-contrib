package mongodb

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
)

// unreachableClient returns a client for an address nothing listens on.
// mongo.Connect does not contact the server, so no MongoDB is needed.
func unreachableClient(t *testing.T) *mongo.Client {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	c, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + addr + "/?directConnection=true").
		SetServerSelectionTimeout(200 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c
}

func TestHookNilClientPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Hook(nil) did not panic")
		}
	}()
	Hook("db", nil)
}

func TestHookName(t *testing.T) {
	if got := Hook("mongo", unreachableClient(t)).Name(); got != "mongo" {
		t.Fatalf("Name = %q", got)
	}
}

func TestHookStartUnreachable(t *testing.T) {
	h := Hook("mongo", unreachableClient(t))
	err := h.Start(t.Context())
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.Unavailable || e.Reason != ReasonUnavailable || !e.Retryable() {
		t.Fatalf("Start = %v, want retryable Unavailable", err)
	}
	if e.Unwrap() == nil {
		t.Fatal("driver error lost")
	}
	if err := h.(health.Checker).Check(t.Context()); errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("Check = %v, want Unavailable", err)
	}
}

func TestHookCheckKeepsContextErrors(t *testing.T) {
	h := Hook("mongo", unreachableClient(t)).(health.Checker)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.Check(ctx); !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
		t.Fatalf("Check with canceled ctx = %v (code %v)", err, errorx.CodeOf(err))
	}

	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := h.Check(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) != errorx.DeadlineExceeded {
		t.Fatalf("Check with expiring ctx = %v (code %v)", err, errorx.CodeOf(err))
	}
}

func TestHookStopDisconnects(t *testing.T) {
	h := Hook("mongo", unreachableClient(t))
	if err := h.Stop(t.Context()); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if err := h.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	err := h.(health.Checker).Check(t.Context())
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e.Code != errorx.FailedPrecondition || e.Reason != ReasonDisconnected || e.Retryable() {
		t.Fatalf("Check after Stop = %v, want non-retryable Unavailable", err)
	}
	if !errors.Is(err, mongo.ErrClientDisconnected) {
		t.Fatalf("cause lost: %v", err)
	}
}

func TestHookFailsAppStart(t *testing.T) {
	a := app.New(app.WithHooks(Hook("mongo", unreachableClient(t))))
	if err := a.Start(t.Context()); errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("App.Start = %v, want the ping failure", err)
	}
	if a.State() != app.StateFailed {
		t.Fatalf("state = %v", a.State())
	}
}
