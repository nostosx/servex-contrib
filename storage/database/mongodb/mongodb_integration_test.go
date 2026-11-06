//go:build integration

package mongodb

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"
	"uuid"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/observability/tracing"
)

// integrationURI returns SERVEX_IT_MONGODB_URI, skipping the test when unset.
func integrationURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("SERVEX_IT_MONGODB_URI")
	if uri == "" {
		t.Skip("SERVEX_IT_MONGODB_URI not set")
	}
	return uri
}

func TestIntegrationHookLifecycle(t *testing.T) {
	uri := integrationURI(t)
	tr, m := &recTracer{}, newRecMeter()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetMonitor(Monitor(tr, m)).
		SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}

	h := Hook("mongo", client)
	a := app.New(app.WithHooks(h))
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("App.Start = %v", err)
	}
	if err := a.Check(t.Context()); err != nil {
		t.Fatalf("App.Check = %v", err)
	}

	coll := client.Database("servex_it").Collection("orders_" + uuid.NewV7().String())
	dropped := false
	t.Cleanup(func() {
		if !dropped {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := coll.Drop(ctx); err != nil {
				t.Errorf("cleanup collection: %v", err)
			}
		}
	})

	parentCtx, parent := tr.Start(t.Context(), "parent", tracing.SpanKindInternal)
	if _, err := coll.InsertOne(parentCtx, bson.D{{Key: "_id", Value: 1}, {Key: "item", Value: "book"}}); err != nil {
		t.Fatal(err)
	}
	// A write error is reported inside a successful reply, so the command
	// succeeds from the monitor's point of view.
	if _, err := coll.InsertOne(t.Context(), bson.D{{Key: "_id", Value: 1}}); err == nil {
		t.Fatal("duplicate insert succeeded")
	}
	var doc bson.M
	if err := coll.FindOne(t.Context(), bson.D{{Key: "_id", Value: 1}}).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	// An unknown command fails on the server.
	if err := client.Database("servex_it").RunCommand(t.Context(), bson.D{{Key: "servexNoSuchCommand", Value: 1}}).Err(); err == nil {
		t.Fatal("unknown command succeeded")
	}

	var inserts, finds, failed []*recSpan
	for _, s := range tr.all() {
		if s.attrs[attrSystem] != "mongodb" {
			continue
		}
		if s.kind != tracing.SpanKindClient {
			t.Errorf("span %s kind = %v", s.name, s.kind)
		}
		if s.endCount() != 1 {
			t.Errorf("span %s ended %d times", s.name, s.endCount())
		}
		switch s.name {
		case "insert":
			inserts = append(inserts, s)
		case "find":
			finds = append(finds, s)
		case "servexNoSuchCommand":
			failed = append(failed, s)
		}
	}
	if len(inserts) != 2 || len(finds) != 1 || len(failed) != 1 {
		t.Fatalf("spans: %d insert, %d find, %d failed", len(inserts), len(finds), len(failed))
	}
	for _, s := range slices.Concat(inserts, finds) {
		if s.attrs[attrName] != "servex_it" || s.attrs[attrCollection] != coll.Name() || len(s.errs) != 0 {
			t.Errorf("span %s attrs %v errors %v", s.name, s.attrs, s.errs)
		}
	}
	if tracing.SpanFromContext(inserts[0].parent) != parent {
		t.Error("insert span is not a child of the operation span")
	}
	if len(failed[0].errs) != 1 {
		t.Errorf("failed command errors = %v", failed[0].errs)
	}
	if got := m.count(metricOps, "mongodb", "insert", "ok"); got != 2 {
		t.Errorf("insert ok count = %v", got)
	}
	if got := m.count(metricOps, "mongodb", "servexNoSuchCommand", "error"); got != 1 {
		t.Errorf("failed command count = %v", got)
	}
	if got := m.count(metricOps, "mongodb", "ping", "ok"); got < 2 {
		t.Errorf("ping ok count = %v, want Start and Check pings", got)
	}
	if len(m.observations(metricDuration, "mongodb", "find")) != 1 {
		t.Error("find duration not observed")
	}

	if err := coll.Drop(t.Context()); err != nil {
		t.Fatal(err)
	}
	dropped = true
	if err := a.Stop(t.Context()); err != nil {
		t.Fatalf("App.Stop = %v", err)
	}
	if err := h.(health.Checker).Check(t.Context()); !errors.Is(err, mongo.ErrClientDisconnected) {
		t.Fatalf("Check after Stop = %v", err)
	}
}
