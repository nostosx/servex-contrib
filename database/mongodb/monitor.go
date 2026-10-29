package mongodb

import (
	"context"
	"errors"
	"sync"

	noopmetrics "github.com/nostosx/servex-contrib/metrics/noop"
	nooptracing "github.com/nostosx/servex-contrib/tracing/noop"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// system is the db.system attribute value and the system metric label.
const system = "mongodb"

// Span attribute keys.
const (
	attrSystem     = "db.system"
	attrOperation  = "db.operation"
	attrName       = "db.name"
	attrCollection = "db.mongodb.collection"
)

// errUnknownFailure stands in for the missing error of a failed command event.
var errUnknownFailure = errors.New("mongodb: command failed")

// Metric names, shared with the database/sql instrumentation of the core
// storage/database package so that dashboards cover both.
const (
	metricOps      = "db_client_operations_total"
	metricDuration = "db_client_operation_duration_seconds"
)

// Monitor returns a command monitor for options.Client().SetMonitor that
// traces and measures every command the driver sends.
//
// Each command produces a tracing.SpanKindClient span named after the command
// ("find", "insert", "aggregate", ...) with the attributes db.system
// ("mongodb"), db.operation (the command name), db.name (the database) and,
// for commands addressing a collection, db.mongodb.collection. The span is
// started from the context of the operation, so it is a child of the span
// in that context; it ends when the command succeeds or fails, recording the
// failure. Command documents are not recorded, as they may hold user data.
//
// Each finished command also updates the metrics
// db_client_operations_total{system,operation,status} (status "ok" or
// "error") and db_client_operation_duration_seconds{system,operation}
// (metrics.DefaultBuckets, seconds, using the duration measured by the
// driver), the same metrics the core storage/database package records for
// SQL databases. The metrics receive the context of the command span, so a
// meter recording exemplars links them to that span; a finished event
// without a started event uses the context of the finished event.
//
// Started and finished events are correlated by connection and request ID,
// so the monitor is safe for concurrent commands. A nil t or m disables
// tracing or metrics respectively.
func Monitor(t tracing.Tracer, m metrics.Meter) *event.CommandMonitor {
	return newMonitor(t, m).commandMonitor()
}

// newMonitor returns a monitor recording to t and m; nil disables either.
func newMonitor(t tracing.Tracer, m metrics.Meter) *monitor {
	if t == nil {
		t = nooptracing.New()
	}
	if m == nil {
		m = noopmetrics.New()
	}
	return &monitor{
		tracer: t,
		ops: m.Counter(metricOps,
			"Total number of database client operations.",
			"system", "operation", "status"),
		durations: m.Histogram(metricDuration,
			"Duration of database client operations in seconds.",
			metrics.DefaultBuckets, "system", "operation"),
		spans: make(map[spanKey]pendingCommand),
	}
}

// commandMonitor returns the driver callbacks of m.
func (m *monitor) commandMonitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started:   m.started,
		Succeeded: m.succeeded,
		Failed:    m.failed,
	}
}

// spanKey identifies an in-flight command. Request IDs are unique per
// process in the driver; the connection ID is included so that correlation
// stays correct even if they were only unique per connection.
type spanKey struct {
	conn string
	id   int64
}

// pendingCommand is a command whose span has not ended.
type pendingCommand struct {
	ctx  context.Context // context holding span
	span tracing.Span
}

// monitor holds the spans of in-flight commands.
type monitor struct {
	tracer    tracing.Tracer
	ops       metrics.Counter
	durations metrics.Histogram

	mu    sync.Mutex
	spans map[spanKey]pendingCommand
}

func (m *monitor) started(ctx context.Context, e *event.CommandStartedEvent) {
	spanCtx, span := m.tracer.Start(ctx, e.CommandName, tracing.SpanKindClient)
	span.SetAttribute(attrSystem, system)
	span.SetAttribute(attrOperation, e.CommandName)
	if e.DatabaseName != "" {
		span.SetAttribute(attrName, e.DatabaseName)
	}
	if coll, ok := collection(e.Command); ok {
		span.SetAttribute(attrCollection, coll)
	}
	m.mu.Lock()
	m.spans[spanKey{conn: e.ConnectionID, id: e.RequestID}] = pendingCommand{ctx: spanCtx, span: span}
	m.mu.Unlock()
}

func (m *monitor) succeeded(ctx context.Context, e *event.CommandSucceededEvent) {
	m.finish(ctx, &e.CommandFinishedEvent, nil)
}

func (m *monitor) failed(ctx context.Context, e *event.CommandFailedEvent) {
	err := e.Failure
	if err == nil {
		// Never expected from the driver, but a failed command must still
		// count as an error.
		err = errUnknownFailure
	}
	m.finish(ctx, &e.CommandFinishedEvent, err)
}

// finish ends the span of the command and records its metrics. A finished
// event without a matching started event still updates the metrics, with
// the context ctx of the finished event.
func (m *monitor) finish(ctx context.Context, e *event.CommandFinishedEvent, err error) {
	key := spanKey{conn: e.ConnectionID, id: e.RequestID}
	m.mu.Lock()
	cmd, ok := m.spans[key]
	delete(m.spans, key)
	m.mu.Unlock()
	if ok {
		ctx = cmd.ctx
	}

	status := "ok"
	if err != nil {
		status = "error"
	}
	m.ops.With(system, e.CommandName, status).Add(ctx, 1)
	m.durations.With(system, e.CommandName).Observe(ctx, e.Duration.Seconds())

	if ok {
		if err != nil {
			cmd.span.RecordError(err)
		}
		cmd.span.End()
	}
}

// inFlight returns the number of commands whose span has not ended.
func (m *monitor) inFlight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.spans)
}

// collection returns the collection addressed by cmd, when the command is
// known to address one. For most such commands the collection is the string
// value of the first element; getMore names it in its "collection" field.
func collection(cmd bson.Raw) (string, bool) {
	first, err := cmd.IndexErr(0)
	if err != nil {
		return "", false
	}
	key, err := first.KeyErr()
	if err != nil {
		return "", false
	}
	switch key {
	case "getMore":
		v, err := cmd.LookupErr("collection")
		if err != nil {
			return "", false
		}
		return v.StringValueOK()
	case "aggregate", "count", "distinct", "find", "findAndModify", "insert", "update", "delete",
		"mapReduce", "create", "drop", "collMod", "createIndexes", "dropIndexes", "listIndexes",
		"killCursors", "validate", "collStats", "compact":
		v, err := first.ValueErr()
		if err != nil {
			return "", false
		}
		// aggregate: 1 runs a database-level pipeline and has no collection.
		return v.StringValueOK()
	default:
		return "", false
	}
}
