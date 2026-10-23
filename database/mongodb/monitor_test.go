package mongodb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

func rawCmd(t testing.TB, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func startedEvent(t testing.TB, conn string, id int64, db string, cmd bson.D) *event.CommandStartedEvent {
	var raw bson.Raw
	name := ""
	if len(cmd) > 0 {
		raw = rawCmd(t, cmd)
		name = cmd[0].Key
	}
	return &event.CommandStartedEvent{
		Command: raw, DatabaseName: db, CommandName: name, RequestID: id, ConnectionID: conn,
	}
}

func finished(conn string, id int64, db, name string, d time.Duration) event.CommandFinishedEvent {
	return event.CommandFinishedEvent{
		Duration: d, CommandName: name, DatabaseName: db, RequestID: id, ConnectionID: conn,
	}
}

func newTestMonitor() (*event.CommandMonitor, *recTracer, *recMeter) {
	tr, m := &recTracer{}, newRecMeter()
	return Monitor(tr, m), tr, m
}

func TestMetricDefinitions(t *testing.T) {
	_, _, m := newTestMonitor()
	if got := m.labels[metricOps]; !slices.Equal(got, []string{"system", "operation", "status"}) {
		t.Errorf("%s labels = %v", metricOps, got)
	}
	if got := m.labels[metricDuration]; !slices.Equal(got, []string{"system", "operation"}) {
		t.Errorf("%s labels = %v", metricDuration, got)
	}
	if got := m.buckets[metricDuration]; !slices.Equal(got, metrics.DefaultBuckets) {
		t.Errorf("%s buckets = %v", metricDuration, got)
	}
}

type ctxKey struct{}

func TestSucceededCommand(t *testing.T) {
	mon, tr, m := newTestMonitor()
	ctx := context.WithValue(t.Context(), ctxKey{}, "op")

	mon.Started(ctx, startedEvent(t, "localhost:27017[-1]", 7, "shop",
		bson.D{{Key: "find", Value: "orders"}, {Key: "filter", Value: bson.D{{Key: "secret", Value: "x"}}}}))
	spans := tr.all()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	s := spans[0]
	if s.name != "find" || s.kind != tracing.SpanKindClient {
		t.Fatalf("span %q kind %v", s.name, s.kind)
	}
	if s.parent.Value(ctxKey{}) != "op" {
		t.Fatal("span not started from the operation context")
	}
	want := map[string]any{
		attrSystem: "mongodb", attrOperation: "find", attrName: "shop", attrCollection: "orders",
	}
	for k, v := range want {
		if s.attrs[k] != v {
			t.Errorf("attr %s = %v, want %v", k, s.attrs[k], v)
		}
	}
	if len(s.attrs) != len(want) {
		t.Errorf("attrs = %v; the command document must not be recorded", s.attrs)
	}
	if s.endCount() != 0 {
		t.Fatal("span ended before the command finished")
	}

	mon.Succeeded(ctx, &event.CommandSucceededEvent{
		CommandFinishedEvent: finished("localhost:27017[-1]", 7, "shop", "find", 250*time.Millisecond),
	})
	if s.endCount() != 1 || len(s.errs) != 0 {
		t.Fatalf("ended %d times, errors %v", s.endCount(), s.errs)
	}
	if got := m.count(metricOps, "mongodb", "find", "ok"); got != 1 {
		t.Errorf("ok count = %v", got)
	}
	if got := m.observations(metricDuration, "mongodb", "find"); !slices.Equal(got, []float64{0.25}) {
		t.Errorf("durations = %v, want [0.25]", got)
	}
}

func TestFailedCommand(t *testing.T) {
	mon, tr, m := newTestMonitor()
	failure := errors.New("E11000 duplicate key error")

	mon.Started(t.Context(), startedEvent(t, "c1", 1, "shop",
		bson.D{{Key: "insert", Value: "orders"}, {Key: "documents", Value: bson.A{}}}))
	mon.Failed(t.Context(), &event.CommandFailedEvent{
		CommandFinishedEvent: finished("c1", 1, "shop", "insert", time.Second),
		Failure:              failure,
	})

	s := tr.all()[0]
	if s.endCount() != 1 || len(s.errs) != 1 || s.errs[0] != failure {
		t.Fatalf("ended %d, errors %v", s.endCount(), s.errs)
	}
	if got := m.count(metricOps, "mongodb", "insert", "error"); got != 1 {
		t.Errorf("error count = %v", got)
	}
	if got := m.count(metricOps, "mongodb", "insert", "ok"); got != 0 {
		t.Errorf("ok count = %v", got)
	}
	if got := m.observations(metricDuration, "mongodb", "insert"); !slices.Equal(got, []float64{1}) {
		t.Errorf("durations = %v", got)
	}
}

func TestFailedCommandWithoutFailure(t *testing.T) {
	mon, tr, m := newTestMonitor()
	mon.Started(t.Context(), startedEvent(t, "c1", 1, "db", bson.D{{Key: "ping", Value: 1}}))
	mon.Failed(t.Context(), &event.CommandFailedEvent{CommandFinishedEvent: finished("c1", 1, "db", "ping", 0)})
	if s := tr.all()[0]; len(s.errs) != 1 {
		t.Fatalf("errors = %v, want a recorded error", s.errs)
	}
	if got := m.count(metricOps, "mongodb", "ping", "error"); got != 1 {
		t.Errorf("error count = %v", got)
	}
}

func TestFinishedWithoutStarted(t *testing.T) {
	mon, tr, m := newTestMonitor()
	mon.Succeeded(t.Context(), &event.CommandSucceededEvent{
		CommandFinishedEvent: finished("c1", 99, "db", "find", time.Millisecond),
	})
	if len(tr.all()) != 0 {
		t.Fatal("unexpected span")
	}
	if got := m.count(metricOps, "mongodb", "find", "ok"); got != 1 {
		t.Errorf("ok count = %v", got)
	}
}

func TestCorrelationByConnectionAndRequestID(t *testing.T) {
	mon, tr, _ := newTestMonitor()
	ctx := t.Context()
	mon.Started(ctx, startedEvent(t, "a", 1, "db", bson.D{{Key: "find", Value: "x"}}))
	mon.Started(ctx, startedEvent(t, "b", 1, "db", bson.D{{Key: "find", Value: "y"}}))
	mon.Started(ctx, startedEvent(t, "a", 2, "db", bson.D{{Key: "find", Value: "z"}}))
	spans := tr.all()

	// Finish out of order; each event must end exactly its own span.
	mon.Failed(ctx, &event.CommandFailedEvent{CommandFinishedEvent: finished("b", 1, "db", "find", 0), Failure: errors.New("boom")})
	if spans[1].endCount() != 1 || spans[0].endCount() != 0 || spans[2].endCount() != 0 {
		t.Fatal("wrong span ended for connection b")
	}
	if len(spans[1].errs) != 1 || len(spans[0].errs) != 0 {
		t.Fatal("error recorded on the wrong span")
	}
	mon.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: finished("a", 2, "db", "find", 0)})
	mon.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: finished("a", 1, "db", "find", 0)})
	// A duplicate finished event must not end a span twice.
	mon.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: finished("a", 1, "db", "find", 0)})
	for i, s := range spans {
		if s.endCount() != 1 {
			t.Errorf("span %d ended %d times", i, s.endCount())
		}
	}
}

func TestConcurrentCommands(t *testing.T) {
	tr, m := &recTracer{}, newRecMeter()
	inner := newMonitor(tr, m)
	mon := inner.commandMonitor()

	raw := rawCmd(t, bson.D{{Key: "find", Value: "c"}})
	const workers, perWorker = 16, 200
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			conn := fmt.Sprintf("conn-%d", w)
			for i := range perWorker {
				id := int64(w*perWorker + i)
				mon.Started(t.Context(), &event.CommandStartedEvent{ConnectionID: conn, RequestID: id, DatabaseName: "db", CommandName: "find", Command: raw})
				fin := finished(conn, id, "db", "find", time.Millisecond)
				if i%2 == 0 {
					mon.Succeeded(t.Context(), &event.CommandSucceededEvent{CommandFinishedEvent: fin})
				} else {
					mon.Failed(t.Context(), &event.CommandFailedEvent{CommandFinishedEvent: fin, Failure: errors.New("x")})
				}
			}
		})
	}
	wg.Wait()

	spans := tr.all()
	if len(spans) != workers*perWorker {
		t.Fatalf("spans = %d", len(spans))
	}
	for _, s := range spans {
		if s.endCount() != 1 {
			t.Fatalf("span ended %d times", s.endCount())
		}
	}
	if n := inner.inFlight(); n != 0 {
		t.Fatalf("%d spans leaked", n)
	}
	half := float64(workers * perWorker / 2)
	if m.count(metricOps, "mongodb", "find", "ok") != half || m.count(metricOps, "mongodb", "find", "error") != half {
		t.Fatal("wrong operation counts")
	}
}

func TestNilTracerAndMeter(t *testing.T) {
	mon := Monitor(nil, nil)
	mon.Started(t.Context(), startedEvent(t, "c", 1, "db", bson.D{{Key: "find", Value: "x"}}))
	mon.Succeeded(t.Context(), &event.CommandSucceededEvent{CommandFinishedEvent: finished("c", 1, "db", "find", 0)})
}

func TestCollection(t *testing.T) {
	tests := []struct {
		name string
		cmd  bson.D
		want string
	}{
		{"find", bson.D{{Key: "find", Value: "users"}}, "users"},
		{"insert", bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{}}}, "users"},
		{"update", bson.D{{Key: "update", Value: "users"}}, "users"},
		{"delete", bson.D{{Key: "delete", Value: "users"}}, "users"},
		{"findAndModify", bson.D{{Key: "findAndModify", Value: "users"}}, "users"},
		{"aggregate", bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}}, "users"},
		{"aggregate database", bson.D{{Key: "aggregate", Value: 1}, {Key: "pipeline", Value: bson.A{}}}, ""},
		{"count", bson.D{{Key: "count", Value: "users"}}, "users"},
		{"distinct", bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "a"}}, "users"},
		{"createIndexes", bson.D{{Key: "createIndexes", Value: "users"}}, "users"},
		{"drop", bson.D{{Key: "drop", Value: "users"}}, "users"},
		{"getMore", bson.D{{Key: "getMore", Value: int64(42)}, {Key: "collection", Value: "users"}}, "users"},
		{"getMore without collection", bson.D{{Key: "getMore", Value: int64(42)}}, ""},
		{"ping", bson.D{{Key: "ping", Value: 1}}, ""},
		{"createUser", bson.D{{Key: "createUser", Value: "alice"}}, ""},
		{"configureFailPoint", bson.D{{Key: "configureFailPoint", Value: "failCommand"}}, ""},
		{"redacted", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw bson.Raw
			if tt.cmd != nil {
				raw = rawCmd(t, tt.cmd)
			}
			got, ok := collection(raw)
			if got != tt.want || ok != (tt.want != "") {
				t.Fatalf("collection = %q, %v; want %q", got, ok, tt.want)
			}
		})
	}
	if _, ok := collection(bson.Raw{0x01, 0x02}); ok {
		t.Fatal("collection of a malformed document")
	}
}

func TestRedactedCommandHasNoCollection(t *testing.T) {
	mon, tr, _ := newTestMonitor()
	mon.Started(t.Context(), &event.CommandStartedEvent{CommandName: "saslStart", DatabaseName: "admin", RequestID: 1, ConnectionID: "c"})
	s := tr.all()[0]
	if _, ok := s.attrs[attrCollection]; ok {
		t.Fatal("collection set for a redacted command")
	}
	if s.attrs[attrOperation] != "saslStart" || s.attrs[attrName] != "admin" {
		t.Fatalf("attrs = %v", s.attrs)
	}
}
