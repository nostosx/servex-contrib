package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"

	noopmetrics "github.com/nostosx/servex-contrib/metrics/noop"
	nooptracing "github.com/nostosx/servex-contrib/tracing/noop"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// defaultSystem is the db.system value used by OpenDB when WithSystem is not
// given. It is the OpenTelemetry value for an unidentified SQL database.
const defaultSystem = "other_sql"

// Option configures Open and OpenDB.
type Option func(*options)

type options struct {
	tracer tracing.Tracer
	meter  metrics.Meter
	system string
}

// WithTracer sets the tracer used for driver spans. The default is
// nooptracing.New(); nil keeps the default.
func WithTracer(t tracing.Tracer) Option {
	return func(o *options) {
		if t != nil {
			o.tracer = t
		}
	}
}

// WithMeter sets the meter used for driver metrics. The default is
// noopmetrics.New(); nil keeps the default.
func WithMeter(m metrics.Meter) Option {
	return func(o *options) {
		if m != nil {
			o.meter = m
		}
	}
}

// WithSystem sets the database system reported in the db.system span
// attribute and the system metric label, such as "mysql" or "postgresql".
// Open defaults to the driver name; OpenDB defaults to "other_sql".
func WithSystem(name string) Option {
	return func(o *options) { o.system = name }
}

// Open opens a database like sql.Open, with the driver instrumented for
// tracing and metrics. The driver registered as driverName is looked up
// through sql.Open; if it implements driver.DriverContext, its connector is
// created with OpenConnector, otherwise connections are opened with
// driver.Driver.Open(dsn). Like sql.Open, Open does not connect: use
// PingContext or Hook to verify the connection.
func Open(driverName, dsn string, opts ...Option) (*sql.DB, error) {
	probe, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("database: open %q: %w", driverName, err)
	}
	d := probe.Driver()
	// The probe never connected; closing it only releases its connector.
	_ = probe.Close()

	var c driver.Connector
	if dc, ok := d.(driver.DriverContext); ok {
		if c, err = dc.OpenConnector(dsn); err != nil {
			return nil, fmt.Errorf("database: open connector %q: %w", driverName, err)
		}
	} else {
		c = dsnConnector{dsn: dsn, driver: d}
	}
	return OpenDB(c, slices.Concat([]Option{WithSystem(driverName)}, opts)...), nil
}

// OpenDB opens a database from a connector like sql.OpenDB, with the
// connector instrumented for tracing and metrics.
//
// Exec, Query, Prepare, Begin, Commit and Rollback produce a
// tracing.SpanKindClient span named after the operation ("exec", "query",
// "prepare", "begin", "commit" or "rollback") with the db.system attribute,
// plus db.statement for statements, and update the metrics
// db_client_operations_total{system,operation,status} (status "ok" or
// "error") and db_client_operation_duration_seconds{system,operation}.
// Query spans end when the driver returns its rows, not when they are closed.
// Statements are recorded verbatim; use parameterized SQL and configure the
// tracer's handling of sensitive literals. Bound argument values are not recorded.
//
// A driver.ErrSkip returned by the driver is not an error: the span records
// no error and no metric is updated, because database/sql retries the
// operation through another path that is measured itself.
//
// The wrapped connections expose an optional driver interface only when the
// underlying connection implements it, so database/sql takes the same code
// paths as with the bare driver. The only interfaces always exposed are those
// whose absence database/sql handles exactly like a pass-through
// implementation (driver.Pinger, driver.ConnPrepareContext,
// driver.ConnBeginTx, driver.NamedValueChecker, driver.StmtExecContext and
// driver.StmtQueryContext); the wrapper reproduces the database/sql fallback
// for them. The raw connection seen through sql.Conn.Raw has an
// Unwrap() driver.Conn method returning the driver's own connection.
func OpenDB(c driver.Connector, opts ...Option) *sql.DB {
	o := options{tracer: nooptracing.New(), meter: noopmetrics.New(), system: defaultSystem}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return sql.OpenDB(&connector{base: c, in: newInstrument(o)})
}

// HookOption configures a hook created by Hook.
type HookOption func(*hookOptions)

type hookOptions struct {
	readiness bool
}

// WithReadiness sets whether the hook takes part in app readiness through a
// Check method that pings the database. The default is true. Disable it when
// the service can still serve some requests while the database is down, so
// that a database outage does not take every instance out of rotation.
func WithReadiness(enabled bool) HookOption {
	return func(o *hookOptions) { o.readiness = enabled }
}

// Hook returns an app.Hook named name that pings db on Start, failing the
// start if the database is unreachable, and closes db on Stop. Unless
// WithReadiness(false) is given, the returned value also implements
// Check(ctx) error, which pings db, so it takes part in app.App readiness and
// can be used as a health.Checker. Start pings db either way. Hook takes
// lifecycle ownership of db; it panics if db is nil.
func Hook(name string, db *sql.DB, opts ...HookOption) app.Hook {
	if db == nil {
		panic("database: Hook with nil *sql.DB")
	}
	o := hookOptions{readiness: true}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	h := &dbHook{name: name, db: db}
	if o.readiness {
		return &readyDBHook{h}
	}
	return h
}

// dbHook implements app.Hook for a *sql.DB.
type dbHook struct {
	name string
	db   *sql.DB
}

func (h *dbHook) Name() string { return h.name }

func (h *dbHook) Start(ctx context.Context) error { return h.ping(ctx) }

func (h *dbHook) Stop(context.Context) error {
	if err := h.db.Close(); err != nil {
		return fmt.Errorf("database: close %q: %w", h.name, err)
	}
	return nil
}

// ping pings the database.
func (h *dbHook) ping(ctx context.Context) error {
	if err := h.db.PingContext(ctx); err != nil {
		return fmt.Errorf("database: ping %q: %w", h.name, err)
	}
	return nil
}

// readyDBHook is a dbHook that also takes part in readiness.
type readyDBHook struct{ *dbHook }

// Check pings the database.
func (h *readyDBHook) Check(ctx context.Context) error { return h.ping(ctx) }

// Tx runs fn in a transaction. If fn returns nil the transaction is
// committed; if fn returns an error it is rolled back and the error is
// returned, joined with the rollback error if rolling back fails for a reason
// other than sql.ErrTxDone. If fn panics (or calls runtime.Goexit) the
// rollback is attempted before the panic or goroutine exit continues. Rollback
// errors are discarded in that case so the original control flow is preserved.
func Tx(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("database: begin: %w", err)
	}
	finished := false
	defer func() {
		if !finished {
			// Roll back without replacing fn's panic or goroutine exit.
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		finished = true
		if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("database: rollback: %w", rerr))
		}
		return err
	}
	finished = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("database: commit: %w", err)
	}
	return nil
}

// connector instruments the connections of a driver.Connector.
type connector struct {
	base driver.Connector
	in   *instrument
}

// Connect opens an instrumented connection.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return wrapConn(conn, c.in), nil
}

// Driver returns the underlying driver, so that sql.DB.Driver reports the
// driver the caller chose.
func (c *connector) Driver() driver.Driver { return c.base.Driver() }

// Close closes the underlying connector if it implements io.Closer, which
// database/sql does on sql.DB.Close.
func (c *connector) Close() error {
	if cl, ok := c.base.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}

// dsnConnector is a driver.Connector for drivers without
// driver.DriverContext, mirroring the one used by sql.Open.
type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) { return c.driver.Open(c.dsn) }

func (c dsnConnector) Driver() driver.Driver { return c.driver }
