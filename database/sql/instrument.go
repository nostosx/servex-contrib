package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// Operation names used for span names and the operation metric label.
const (
	opExec     = "exec"
	opQuery    = "query"
	opPrepare  = "prepare"
	opBegin    = "begin"
	opCommit   = "commit"
	opRollback = "rollback"
)

// Span attribute keys.
const (
	attrSystem    = "db.system"
	attrStatement = "db.statement"
)

// instrument records spans and metrics for driver operations.
type instrument struct {
	tracer    tracing.Tracer
	system    string
	ops       metrics.Counter
	durations metrics.Histogram
}

func newInstrument(o options) *instrument {
	return &instrument{
		tracer: o.tracer,
		system: o.system,
		ops: o.meter.Counter("db_client_operations_total",
			"Total number of database client operations.",
			"system", "operation", "status"),
		durations: o.meter.Histogram("db_client_operation_duration_seconds",
			"Duration of database client operations in seconds.",
			metrics.DefaultBuckets, "system", "operation"),
	}
}

// start begins operation op. statement is recorded when non-empty. The
// returned context carries the span and must be passed to the driver; the
// returned function must be called with the outcome of the operation.
func (in *instrument) start(ctx context.Context, op, statement string) (context.Context, func(error)) {
	begin := time.Now()
	ctx, span := in.tracer.Start(ctx, op, tracing.SpanKindClient)
	span.SetAttribute(attrSystem, in.system)
	if statement != "" {
		span.SetAttribute(attrStatement, statement)
	}
	return ctx, func(err error) {
		defer span.End()
		if errors.Is(err, driver.ErrSkip) {
			return
		}
		status := "ok"
		if err != nil {
			status = "error"
			span.RecordError(err)
		}
		// The span context lets a meter link the observation to the span.
		in.ops.With(in.system, op, status).Add(ctx, 1)
		in.durations.With(in.system, op).Observe(ctx, time.Since(begin).Seconds())
	}
}
