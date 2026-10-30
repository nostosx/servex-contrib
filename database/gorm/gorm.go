// Package gorm connects native GORM databases to servex observability and
// lifecycle. Applications choose their GORM dialector and retain *gorm.DB.
package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdkgorm "gorm.io/gorm"

	sqldb "github.com/nostosx/servex-contrib/database/sql"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// Plugin instruments GORM create/query/update/delete/row/raw callbacks.
// Install once with db.Use before concurrent use. The injected tracer and meter
// must be concurrency-safe; nil disables the corresponding signal.
type Plugin struct {
	tracer    tracing.Tracer
	ops       metrics.Counter
	durations metrics.Histogram
}

var _ sdkgorm.Plugin = (*Plugin)(nil)

// NewPlugin uses bounded operation labels and the configured dialect name.
// SQL text, parameters, DSNs, table names and raw error messages are never
// recorded. Do not also instrument the underlying SQL driver unless distinct
// ORM and driver spans/metrics are intentional. DryRun calls are not recorded.
func NewPlugin(tracer tracing.Tracer, meter metrics.Meter) *Plugin {
	p := &Plugin{tracer: tracer}
	if meter != nil {
		p.ops = meter.Counter("gorm_client_operations_total", "Total GORM operations.", "system", "operation", "status")
		p.durations = meter.Histogram("gorm_client_operation_duration_seconds", "GORM operation duration in seconds.", metrics.DefaultBuckets, "system", "operation")
	}
	return p
}

// Name identifies this plugin in GORM's plugin registry.
func (*Plugin) Name() string { return "servex:observability" }

// Initialize installs callbacks on db. A failed initialization must discard db;
// GORM callback registration is not transactional. Sessions share callbacks.
func (p *Plugin) Initialize(db *sdkgorm.DB) error {
	if db == nil || db.Dialector == nil {
		return errors.New("gorm: database and dialector are required")
	}
	// The concrete GORM callback processor is private; keep registration typed
	// through bound methods rather than reflecting over the SDK internals.
	registrations := []struct {
		operation string
		before    func(string, func(*sdkgorm.DB)) error
		after     func(string, func(*sdkgorm.DB)) error
	}{
		{"create", db.Callback().Create().Before("*").Register, db.Callback().Create().After("*").Register},
		{"query", db.Callback().Query().Before("*").Register, db.Callback().Query().After("*").Register},
		{"update", db.Callback().Update().Before("*").Register, db.Callback().Update().After("*").Register},
		{"delete", db.Callback().Delete().Before("*").Register, db.Callback().Delete().After("*").Register},
		{"row", db.Callback().Row().Before("*").Register, db.Callback().Row().After("*").Register},
		{"raw", db.Callback().Raw().Before("*").Register, db.Callback().Raw().After("*").Register},
	}
	for _, registration := range registrations {
		if err := registration.before("servex:before", p.before(registration.operation)); err != nil {
			return fmt.Errorf("gorm: register %s start: %w", registration.operation, err)
		}
		if err := registration.after("servex:after", p.after(registration.operation)); err != nil {
			return fmt.Errorf("gorm: register %s end: %w", registration.operation, err)
		}
	}
	return nil
}

type operationKey struct{}

type operationState struct {
	parent context.Context
	span   tracing.Span
	start  time.Time
}

func (p *Plugin) before(operation string) func(*sdkgorm.DB) {
	return func(db *sdkgorm.DB) {
		if db.DryRun {
			return
		}
		ctx := db.Statement.Context
		state := &operationState{parent: ctx, start: time.Now()}
		if p.tracer != nil {
			ctx, state.span = p.tracer.Start(ctx, "gorm."+operation, tracing.SpanKindClient)
			if state.span != nil {
				state.span.SetAttribute("db.system", db.Name())
				state.span.SetAttribute("db.operation", operation)
			}
		}
		db.Statement.Context = context.WithValue(ctx, operationKey{}, state)
	}
}

var errOperation = errors.New("gorm operation failed")

func (p *Plugin) after(operation string) func(*sdkgorm.DB) {
	return func(db *sdkgorm.DB) {
		if db.DryRun {
			return
		}
		state, ok := db.Statement.Context.Value(operationKey{}).(*operationState)
		if !ok {
			return
		}
		ctx := db.Statement.Context
		defer func() {
			db.Statement.Context = state.parent
			if state.span != nil {
				state.span.End()
			}
		}()
		status := "ok"
		if db.Error != nil && !errors.Is(db.Error, sdkgorm.ErrRecordNotFound) {
			status = "error"
			if state.span != nil {
				state.span.RecordError(errOperation)
			}
		}
		if p.ops != nil {
			p.ops.With(db.Name(), operation, status).Add(ctx, 1)
			p.durations.With(db.Name(), operation).Observe(ctx, time.Since(state.start).Seconds())
		}
	}
}

// Hook connects db's native *sql.DB to App startup, shutdown and readiness using
// the existing SQL adapter. Start and Check ping; Stop closes the pool. Give the
// pool exactly one lifecycle owner. Transactions and non-pool handles are rejected.
func Hook(name string, db *sdkgorm.DB) (app.Hook, error) {
	if db == nil || db.Config == nil {
		return nil, errors.New("gorm: database is required")
	}
	connection := db.ConnPool
	if db.Statement != nil && db.Statement.ConnPool != nil {
		connection = db.Statement.ConnPool
	}
	if _, transaction := connection.(sdkgorm.TxCommitter); transaction {
		return nil, errors.New("gorm: lifecycle hook requires a root database, not a transaction")
	}
	pool, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm: get connection pool: %w", err)
	}
	if pool == nil {
		return nil, errors.New("gorm: connection pool is required")
	}
	return sqldb.Hook(name, pool), nil
}
