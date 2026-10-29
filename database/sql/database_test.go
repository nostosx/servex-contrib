package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	noopmetrics "github.com/nostosx/servex-contrib/metrics/noop"
	nooptracing "github.com/nostosx/servex-contrib/tracing/noop"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

const (
	metricOps      = "db_client_operations_total"
	metricDuration = "db_client_operation_duration_seconds"
)

// instrumented opens a DB over c with a recording tracer and meter.
func instrumented(t *testing.T, c driver.Connector, opts ...Option) (*sql.DB, *recTracer, *recMeter) {
	t.Helper()
	tr, m := &recTracer{}, newRecMeter()
	db := OpenDB(c, slices.Concat([]Option{WithTracer(tr), WithMeter(m), WithSystem("fakesql")}, opts)...)
	t.Cleanup(func() { _ = db.Close() })
	return db, tr, m
}

// raw runs f with the driver connection of a fresh sql.Conn.
func raw(t *testing.T, db *sql.DB, f func(dc any)) {
	t.Helper()
	c, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Raw(func(dc any) error { f(dc); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestMetricDefinitions(t *testing.T) {
	_, _, m := instrumented(t, newFullConnector())
	if got := m.labels[metricOps]; !slices.Equal(got, []string{"system", "operation", "status"}) {
		t.Errorf("%s labels = %v", metricOps, got)
	}
	if got := m.labels[metricDuration]; !slices.Equal(got, []string{"system", "operation"}) {
		t.Errorf("%s labels = %v", metricDuration, got)
	}
	if got := m.buckets[metricDuration]; !slices.Equal(got, metrics.DefaultBuckets) {
		t.Errorf("%s buckets = %v, want DefaultBuckets", metricDuration, got)
	}
}

func TestInstrumentation(t *testing.T) {
	tests := []struct {
		name      string
		run       func(ctx context.Context, db *sql.DB) error
		wantSpans []spanSnapshot
		wantOK    []string // operations counted with status ok
		wantError []string // operations counted with status error
	}{
		{
			name: "exec",
			run: func(ctx context.Context, db *sql.DB) error {
				_, err := db.ExecContext(ctx, "INSERT 1")
				return err
			},
			wantSpans: []spanSnapshot{{name: "exec", statement: "INSERT 1"}},
			wantOK:    []string{"exec"},
		},
		{
			name: "exec error",
			run: func(ctx context.Context, db *sql.DB) error {
				if _, err := db.ExecContext(ctx, "fail"); !errors.Is(err, errFake) {
					return fmt.Errorf("err = %v, want errFake", err) //nolint:errorlint // err may be nil; this reports a failed expectation, not a cause
				}
				return nil
			},
			wantSpans: []spanSnapshot{{name: "exec", statement: "fail", failed: true}},
			wantError: []string{"exec"},
		},
		{
			name: "query",
			run: func(ctx context.Context, db *sql.DB) error {
				var v string
				if err := db.QueryRowContext(ctx, "SELECT v").Scan(&v); err != nil {
					return err
				}
				if v != "SELECT v" {
					return fmt.Errorf("scanned %q", v)
				}
				return nil
			},
			wantSpans: []spanSnapshot{{name: "query", statement: "SELECT v"}},
			wantOK:    []string{"query"},
		},
		{
			name: "prepared statement",
			run: func(ctx context.Context, db *sql.DB) error {
				st, err := db.PrepareContext(ctx, "UPDATE x")
				if err != nil {
					return err
				}
				defer st.Close()
				if _, err := st.ExecContext(ctx); err != nil {
					return err
				}
				rows, err := st.QueryContext(ctx)
				if err != nil {
					return err
				}
				return rows.Close()
			},
			wantSpans: []spanSnapshot{
				{name: "prepare", statement: "UPDATE x"},
				{name: "exec", statement: "UPDATE x"},
				{name: "query", statement: "UPDATE x"},
			},
			wantOK: []string{"prepare", "exec", "query"},
		},
		{
			name: "prepare error",
			run: func(ctx context.Context, db *sql.DB) error {
				if _, err := db.PrepareContext(ctx, "fail"); !errors.Is(err, errFake) {
					return fmt.Errorf("err = %v, want errFake", err) //nolint:errorlint // err may be nil; this reports a failed expectation, not a cause
				}
				return nil
			},
			wantSpans: []spanSnapshot{{name: "prepare", statement: "fail", failed: true}},
			wantError: []string{"prepare"},
		},
		{
			name: "transaction",
			run: func(ctx context.Context, db *sql.DB) error {
				return Tx(ctx, db, nil, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "INSERT 2")
					return err
				})
			},
			wantSpans: []spanSnapshot{
				{name: "begin"},
				{name: "exec", statement: "INSERT 2"},
				{name: "commit"},
			},
			wantOK: []string{"begin", "exec", "commit"},
		},
		{
			name: "rollback",
			run: func(ctx context.Context, db *sql.DB) error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				return tx.Rollback()
			},
			wantSpans: []spanSnapshot{{name: "begin"}, {name: "rollback"}},
			wantOK:    []string{"begin", "rollback"},
		},
		{
			name: "skip falls back to prepare",
			run: func(ctx context.Context, db *sql.DB) error {
				_, err := db.ExecContext(ctx, "skip me")
				return err
			},
			wantSpans: []spanSnapshot{
				{name: "exec", statement: "skip me"}, // ErrSkip: no error recorded
				{name: "prepare", statement: "skip me"},
				{name: "exec", statement: "skip me"},
			},
			wantOK: []string{"prepare", "exec"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, tr, m := instrumented(t, newFullConnector())
			if err := db.PingContext(t.Context()); err != nil { // open the connection up front
				t.Fatal(err)
			}
			if err := tt.run(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			got := tr.snapshot()
			for i := range tt.wantSpans {
				w := &tt.wantSpans[i]
				w.system, w.kind, w.ended = "fakesql", tracing.SpanKindClient, true
			}
			if !slices.Equal(got, tt.wantSpans) {
				t.Fatalf("spans:\n got  %+v\n want %+v", got, tt.wantSpans)
			}
			for _, op := range []string{opExec, opQuery, opPrepare, opBegin, opCommit, opRollback} {
				wantOK := float64(countOf(tt.wantOK, op))
				wantErr := float64(countOf(tt.wantError, op))
				if got := m.count(metricOps, "fakesql", op, "ok"); got != wantOK {
					t.Errorf("%s{%s,ok} = %v, want %v", metricOps, op, got, wantOK)
				}
				if got := m.count(metricOps, "fakesql", op, "error"); got != wantErr {
					t.Errorf("%s{%s,error} = %v, want %v", metricOps, op, got, wantErr)
				}
				if got := len(m.observations(metricDuration, "fakesql", op)); float64(got) != wantOK+wantErr {
					t.Errorf("%s{%s} has %d observations, want %v", metricDuration, op, got, wantOK+wantErr)
				}
			}
		})
	}
}

func countOf(ops []string, op string) int {
	n := 0
	for _, o := range ops {
		if o == op {
			n++
		}
	}
	return n
}

func TestSpanContextReachesDriver(t *testing.T) {
	c := newFullConnector()
	db, tr, _ := instrumented(t, c)
	ctx, parent := tr.Start(t.Context(), "request", tracing.SpanKindServer)
	defer parent.End()

	if err := Tx(ctx, db, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The driver sees the operation span in its context.
	for _, ev := range []string{"span:begin", "span:exec"} {
		if !c.st.has(ev) {
			t.Errorf("driver did not see %q; events %v", ev, c.st.events())
		}
	}
	// Begin, exec and commit are children of the caller's span.
	for _, s := range tr.snapshot("request") {
		if s.parent != "request" {
			t.Errorf("span %q has parent %q, want request", s.name, s.parent)
		}
	}
}

func TestDurationWithSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFullConnector()
		c.st.sleep = 250 * time.Millisecond
		m := newRecMeter()
		db := OpenDB(c, WithMeter(m), WithSystem("fakesql"))
		defer db.Close()
		if _, err := db.ExecContext(t.Context(), "INSERT"); err != nil {
			t.Fatal(err)
		}
		if got := m.observations(metricDuration, "fakesql", opExec); !slices.Equal(got, []float64{0.25}) {
			t.Fatalf("durations = %v, want [0.25]", got)
		}
	})
}

func TestDefaults(t *testing.T) {
	// Without options the noop tracer and meter are used and the system is
	// "other_sql"; nil options are ignored.
	tr := &recTracer{}
	db := OpenDB(newFullConnector(), nil, WithTracer(nil), WithMeter(nil))
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "INSERT"); err != nil {
		t.Fatal(err)
	}

	db2 := OpenDB(newFullConnector(), WithTracer(tr))
	defer db2.Close()
	if _, err := db2.ExecContext(t.Context(), "INSERT"); err != nil {
		t.Fatal(err)
	}
	if spans := tr.snapshot(); len(spans) != 1 || spans[0].system != defaultSystem {
		t.Fatalf("spans = %+v, want one span with system %q", spans, defaultSystem)
	}
}

// connCase describes a driver connection with a set of optional interfaces.
type connCase struct {
	name                    string
	conn                    func(c *fakeConn) driver.Conn
	execer, queryer         bool
	resetter, validator     bool
	execEvent, queryEvent   string
	beginOptionsUnsupported bool
}

func connCases() []connCase {
	return []connCase{
		{
			name:                    "bare",
			conn:                    func(c *fakeConn) driver.Conn { return c },
			execEvent:               "stmt-exec",
			queryEvent:              "stmt-query",
			beginOptionsUnsupported: true,
		},
		{
			name:   "full",
			conn:   fullConn,
			execer: true, queryer: true, resetter: true, validator: true,
			execEvent: "exec", queryEvent: "query",
		},
		{
			name: "legacy execer and queryer",
			conn: func(c *fakeConn) driver.Conn {
				return struct {
					*fakeConn
					driver.Execer
					driver.Queryer
				}{c, (*fakeExecer)(c), (*fakeQueryer)(c)}
			},
			execer: true, queryer: true,
			execEvent: "legacy-exec", queryEvent: "legacy-query",
			beginOptionsUnsupported: true,
		},
		{
			name: "execer context only",
			conn: func(c *fakeConn) driver.Conn {
				return struct {
					*fakeConn
					driver.ExecerContext
				}{c, (*fakeExecerCtx)(c)}
			},
			execer:                  true,
			execEvent:               "exec",
			queryEvent:              "stmt-query",
			beginOptionsUnsupported: true,
		},
		{
			name: "queryer context and resetter",
			conn: func(c *fakeConn) driver.Conn {
				return struct {
					*fakeConn
					driver.QueryerContext
					driver.SessionResetter
				}{c, (*fakeQueryerCtx)(c), (*fakeResetter)(c)}
			},
			queryer: true, resetter: true,
			execEvent: "stmt-exec", queryEvent: "query",
			beginOptionsUnsupported: true,
		},
		{
			name: "validator only",
			conn: func(c *fakeConn) driver.Conn {
				return struct {
					*fakeConn
					driver.Validator
				}{c, (*fakeValidator)(c)}
			},
			validator: true,
			execEvent: "stmt-exec", queryEvent: "stmt-query",
			beginOptionsUnsupported: true,
		},
		{
			name: "resetter and validator",
			conn: func(c *fakeConn) driver.Conn {
				return struct {
					*fakeConn
					driver.SessionResetter
					driver.Validator
				}{c, (*fakeResetter)(c), (*fakeValidator)(c)}
			},
			resetter: true, validator: true,
			execEvent: "stmt-exec", queryEvent: "stmt-query",
			beginOptionsUnsupported: true,
		},
	}
}

func (cc connCase) connector() *fakeConnector {
	st := &fakeState{}
	return &fakeConnector{st: st, newConn: func() driver.Conn { return cc.conn(&fakeConn{st: st}) }, driver: legacyDriver{}}
}

func TestOptionalInterfacesMirrorDriver(t *testing.T) {
	for _, cc := range connCases() {
		t.Run(cc.name, func(t *testing.T) {
			db, _, _ := instrumented(t, cc.connector())
			raw(t, db, func(dc any) {
				check := func(iface string, got, want bool) {
					if got != want {
						t.Errorf("%s exposed = %v, want %v", iface, got, want)
					}
				}
				_, ok := dc.(driver.ExecerContext)
				check("ExecerContext", ok, cc.execer)
				_, ok = dc.(driver.QueryerContext)
				check("QueryerContext", ok, cc.queryer)
				_, ok = dc.(driver.SessionResetter)
				check("SessionResetter", ok, cc.resetter)
				_, ok = dc.(driver.Validator)
				check("Validator", ok, cc.validator)
				// The legacy interfaces are never exposed: database/sql prefers
				// the context variants, which are emulated on top of them.
				_, ok = dc.(driver.Execer) //nolint:staticcheck // asserting the legacy interface is hidden
				check("Execer", ok, false)
				_, ok = dc.(driver.Queryer) //nolint:staticcheck // asserting the legacy interface is hidden
				check("Queryer", ok, false)
				// Interfaces whose absence database/sql handles like a
				// pass-through are always exposed.
				for name, ok := range map[string]bool{
					"Pinger":             is[driver.Pinger](dc),
					"ConnPrepareContext": is[driver.ConnPrepareContext](dc),
					"ConnBeginTx":        is[driver.ConnBeginTx](dc),
					"NamedValueChecker":  is[driver.NamedValueChecker](dc),
				} {
					check(name, ok, true)
				}
				u, ok := dc.(interface{ Unwrap() driver.Conn })
				if !ok {
					t.Fatal("raw connection has no Unwrap method")
				}
				if u.Unwrap() == nil {
					t.Fatal("Unwrap returned nil")
				}
			})
		})
	}
}

func is[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

func TestDriverPathsAndFallbacks(t *testing.T) {
	for _, cc := range connCases() {
		t.Run(cc.name, func(t *testing.T) {
			c := cc.connector()
			db, tr, _ := instrumented(t, c)
			ctx := t.Context()

			if _, err := db.ExecContext(ctx, "INSERT", 1); err != nil {
				t.Fatalf("Exec: %v", err)
			}
			var v string
			if err := db.QueryRowContext(ctx, "SELECT", 1).Scan(&v); err != nil || v != "SELECT" {
				t.Fatalf("Query = %q, %v", v, err)
			}
			for _, ev := range []string{cc.execEvent + ":INSERT", cc.queryEvent + ":SELECT"} {
				if !c.st.has(ev) {
					t.Errorf("driver event %q missing; events %v", ev, c.st.events())
				}
			}
			// Exec and query spans are produced on every path.
			names := make(map[string]int)
			for _, s := range tr.snapshot() {
				names[s.name]++
				if s.failed {
					t.Errorf("span %q recorded an error", s.name)
				}
			}
			if names[opExec] != 1 || names[opQuery] != 1 {
				t.Errorf("span counts = %v, want one exec and one query", names)
			}

			// Named parameters need a context-aware exec path in the driver.
			_, err := db.ExecContext(ctx, "INSERT", sql.Named("a", 1))
			unsupported := cc.execEvent != "exec"
			if unsupported != (err != nil && strings.Contains(err.Error(), "Named Parameters")) {
				t.Errorf("named parameter exec err = %v, unsupported = %v", err, unsupported)
			}

			// BeginTx with options needs driver.ConnBeginTx.
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			switch {
			case cc.beginOptionsUnsupported:
				if !errors.Is(err, errIsolationLevel) {
					t.Errorf("isolation err = %v, want %v", err, errIsolationLevel)
				}
				if _, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); !errors.Is(err, errReadOnly) {
					t.Errorf("read-only err = %v, want %v", err, errReadOnly)
				}
			case err != nil:
				t.Errorf("BeginTx: %v", err)
			default:
				_ = tx.Rollback()
			}
			if err := Tx(ctx, db, nil, func(tx *sql.Tx) error { return nil }); err != nil {
				t.Errorf("Tx: %v", err)
			}
		})
	}
}

func TestSessionHooksReachDriver(t *testing.T) {
	cc := connCases()[1] // full
	c := cc.connector()
	db, _, _ := instrumented(t, c)
	db.SetMaxOpenConns(1)
	for range 2 {
		if _, err := db.ExecContext(t.Context(), "INSERT"); err != nil {
			t.Fatal(err)
		}
	}
	for _, ev := range []string{"is-valid", "reset"} {
		if !c.st.has(ev) {
			t.Errorf("driver event %q missing; events %v", ev, c.st.events())
		}
	}
}

func TestStatementInterfaces(t *testing.T) {
	tests := []struct {
		name      string
		kind      stmtKind
		connNVC   bool
		wantCC    bool
		wantCheck error
		wantEvent string
	}{
		{name: "legacy", kind: stmtLegacy, wantCheck: driver.ErrSkip},
		{name: "context", kind: stmtCtx, wantCheck: driver.ErrSkip},
		{name: "column converter", kind: stmtCC, wantCC: true, wantCheck: driver.ErrSkip},
		{name: "statement checker", kind: stmtNVC, connNVC: true, wantCheck: errStmtChecker},
		{name: "connection checker", kind: stmtLegacy, connNVC: true, wantCheck: driver.ErrSkip, wantEvent: "conn-nvc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeState{}
			var base driver.Conn = &fakeConn{st: st, stmts: tt.kind}
			if tt.connNVC {
				fc := base.(*fakeConn)
				base = struct {
					*fakeConn
					driver.NamedValueChecker
				}{fc, (*fakeConnNVC)(fc)}
			}
			tr := &recTracer{}
			in := newInstrument(options{tracer: tr, meter: noopmetrics.New(), system: "fakesql"})
			wc := wrapConn(base, in).(driver.ConnPrepareContext)
			s, err := wc.PrepareContext(t.Context(), "SELECT")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if got := is[driver.ColumnConverter](s); got != tt.wantCC { //nolint:staticcheck // the wrapper forwards the legacy interface
				t.Errorf("ColumnConverter exposed = %v, want %v", got, tt.wantCC)
			}
			for name, ok := range map[string]bool{
				"StmtExecContext":   is[driver.StmtExecContext](s),
				"StmtQueryContext":  is[driver.StmtQueryContext](s),
				"NamedValueChecker": is[driver.NamedValueChecker](s),
			} {
				if !ok {
					t.Errorf("%s not exposed", name)
				}
			}
			err = s.(driver.NamedValueChecker).CheckNamedValue(&driver.NamedValue{Ordinal: 1, Value: 1})
			if !errors.Is(err, tt.wantCheck) {
				t.Errorf("CheckNamedValue = %v, want %v", err, tt.wantCheck)
			}
			if tt.wantEvent != "" && !st.has(tt.wantEvent) {
				t.Errorf("event %q missing", tt.wantEvent)
			}

			// The deprecated entry points are instrumented too.
			if _, err := s.Exec(nil); err != nil { //nolint:staticcheck // exercising the legacy entry point
				t.Fatal(err)
			}
			rows, err := s.Query(nil) //nolint:staticcheck // exercising the legacy entry point
			if err != nil {
				t.Fatal(err)
			}
			_ = rows.Close()
			if n := len(tr.snapshot()); n != 3 {
				t.Errorf("recorded %d spans, want prepare, exec and query", n)
			}

			// The legacy statement path rejects named parameters.
			_, err = s.(driver.StmtExecContext).ExecContext(t.Context(), []driver.NamedValue{{Name: "a", Ordinal: 1, Value: 1}})
			if wantErr := tt.kind != stmtCtx; (err != nil) != wantErr {
				t.Errorf("named exec err = %v, want error %v", err, wantErr)
			}
		})
	}
}

func TestCanceledContextOnLegacyPaths(t *testing.T) {
	cc := connCases()[2] // legacy execer and queryer
	in := newInstrument(options{tracer: nooptracing.New(), meter: noopmetrics.New()})
	wc := wrapConn(cc.conn(&fakeConn{st: &fakeState{}}), in)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := wc.(driver.ExecerContext).ExecContext(ctx, "INSERT", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("exec err = %v", err)
	}
	if _, err := wc.(driver.QueryerContext).QueryContext(ctx, "SELECT", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("query err = %v", err)
	}
	if _, err := wc.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT"); !errors.Is(err, context.Canceled) {
		t.Errorf("prepare err = %v", err)
	}
	if _, err := wc.(driver.ConnBeginTx).BeginTx(ctx, driver.TxOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("begin err = %v", err)
	}
	if _, err := wc.Begin(); err != nil { //nolint:staticcheck // exercising the legacy entry point
		t.Errorf("Begin: %v", err)
	}
	if _, err := wc.Prepare("SELECT"); err != nil {
		t.Errorf("Prepare: %v", err)
	}
}

func TestConnectorPassThrough(t *testing.T) {
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	if _, ok := db.Driver().(ctxDriver); !ok {
		t.Errorf("Driver() = %T, want the underlying driver", db.Driver())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !c.closed.Load() {
		t.Error("underlying connector was not closed")
	}

	// A connector without Close is fine as well.
	db2 := OpenDB(struct{ driver.Connector }{newFullConnector()})
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	tests := []struct {
		name         string
		driver       string
		dsn          string
		opts         []Option
		wantSystem   string
		viaConnector bool
		wantErr      bool
	}{
		{name: "driver context", driver: ctxDriverName, dsn: "db1", wantSystem: ctxDriverName, viaConnector: true},
		{name: "legacy driver", driver: legacyDriverName, dsn: "db2", wantSystem: legacyDriverName},
		{name: "system override", driver: ctxDriverName, dsn: "db3", opts: []Option{WithSystem("postgresql")}, wantSystem: "postgresql", viaConnector: true},
		{name: "unknown driver", driver: "servex-database-missing", wantErr: true},
		{name: "connector error", driver: ctxDriverName, dsn: "bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &recTracer{}
			db, err := Open(tt.driver, tt.dsn, slices.Concat([]Option{WithTracer(tr)}, tt.opts)...)
			if tt.wantErr {
				if err == nil || !strings.HasPrefix(err.Error(), "database: ") {
					t.Fatalf("err = %v, want a database error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ExecContext(t.Context(), "INSERT"); err != nil {
				t.Fatal(err)
			}
			raw(t, db, func(dc any) {
				fc := fakeOf(t, dc.(interface{ Unwrap() driver.Conn }).Unwrap())
				if fc.viaConnector != tt.viaConnector || fc.st.dsn != tt.dsn {
					t.Errorf("conn viaConnector=%v dsn=%q; want %v %q", fc.viaConnector, fc.st.dsn, tt.viaConnector, tt.dsn)
				}
			})
			spans := tr.snapshot()
			if len(spans) == 0 || spans[0].system != tt.wantSystem {
				t.Fatalf("spans = %+v, want system %q", spans, tt.wantSystem)
			}
		})
	}
}

// fakeOf extracts the *fakeConn from a fake driver connection.
func fakeOf(t *testing.T, c driver.Conn) *fakeConn {
	t.Helper()
	f, ok := c.(interface{ fake() *fakeConn })
	if !ok {
		t.Fatalf("unexpected driver connection %T", c)
	}
	return f.fake()
}

func TestHook(t *testing.T) {
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	h := Hook("primary", db)
	if h.Name() != "primary" {
		t.Fatalf("Name = %q", h.Name())
	}
	checker, ok := h.(interface{ Check(context.Context) error })
	if !ok {
		t.Fatal("hook does not implement Check")
	}
	ctx := t.Context()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := checker.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}
	c.st.set(func(s *fakeState) { s.pingErr = errFake })
	if err := checker.Check(ctx); !errors.Is(err, errFake) || !strings.Contains(err.Error(), `database: ping "primary"`) {
		t.Fatalf("Check err = %v", err)
	}
	if err := h.Start(ctx); !errors.Is(err, errFake) {
		t.Fatalf("Start err = %v, want errFake", err)
	}
	c.st.set(func(s *fakeState) { s.pingErr = nil })
	if err := h.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := db.PingContext(ctx); err == nil {
		t.Fatal("db still usable after Stop")
	}
	if !c.closed.Load() {
		t.Fatal("connector not closed by Stop")
	}
}

func TestHookWithApp(t *testing.T) {
	tests := []struct {
		name      string
		pingErr   error
		wantStart bool
	}{
		{name: "reachable", wantStart: true},
		{name: "unreachable", pingErr: errFake},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFullConnector()
			c.st.pingErr = tt.pingErr
			db, _, _ := instrumented(t, c)
			a := app.New(app.WithHooks(Hook("db", db)), app.WithLogger(testLogger(slog.New(slog.DiscardHandler))))
			err := a.Start(t.Context())
			if (err == nil) != tt.wantStart {
				t.Fatalf("Start err = %v", err)
			}
			if !tt.wantStart {
				if !errors.Is(err, errFake) {
					t.Fatalf("Start err = %v, want errFake", err)
				}
				return
			}
			if err := a.Check(t.Context()); err != nil {
				t.Fatalf("Check: %v", err)
			}
			if err := a.Stop(t.Context()); err != nil {
				t.Fatalf("Stop: %v", err)
			}
		})
	}
}

func TestHookNilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Hook(nil) did not panic")
		}
	}()
	Hook("db", nil)
}

func TestTx(t *testing.T) {
	errFn := errors.New("fn failed")
	errRollback := errors.New("rollback failed")
	errCommit := errors.New("commit failed")
	tests := []struct {
		name        string
		fn          func(tx *sql.Tx) error
		rollbackErr error
		commitErr   error
		wantErr     []error
		wantEvents  []string
	}{
		{
			name:       "commit",
			fn:         func(*sql.Tx) error { return nil },
			wantEvents: []string{"commit"},
		},
		{
			name:       "rollback on error",
			fn:         func(*sql.Tx) error { return errFn },
			wantErr:    []error{errFn},
			wantEvents: []string{"rollback"},
		},
		{
			name:        "rollback error is joined",
			fn:          func(*sql.Tx) error { return errFn },
			rollbackErr: errRollback,
			wantErr:     []error{errFn, errRollback},
			wantEvents:  []string{"rollback"},
		},
		{
			name:       "commit error",
			fn:         func(*sql.Tx) error { return nil },
			commitErr:  errCommit,
			wantErr:    []error{errCommit},
			wantEvents: []string{"commit"},
		},
		{
			name: "fn finished the transaction itself",
			fn: func(tx *sql.Tx) error {
				_ = tx.Commit()
				return errFn
			},
			wantErr:    []error{errFn},
			wantEvents: []string{"commit"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFullConnector()
			c.st.rollbackErr, c.st.commitErr = tt.rollbackErr, tt.commitErr
			db, _, _ := instrumented(t, c)
			err := Tx(t.Context(), db, nil, tt.fn)
			if len(tt.wantErr) == 0 && err != nil {
				t.Fatalf("err = %v", err)
			}
			for _, want := range tt.wantErr {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want it to wrap %v", err, want)
				}
			}
			var got []string
			for _, ev := range c.st.events() {
				if ev == "commit" || ev == "rollback" {
					got = append(got, ev)
				}
			}
			if !slices.Equal(got, tt.wantEvents) {
				t.Errorf("tx events = %v, want %v", got, tt.wantEvents)
			}
		})
	}
}

func TestTxPanicRollsBackAndRepanics(t *testing.T) {
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	type sentinel struct{ msg string }
	func() {
		defer func() {
			r := recover()
			if s, ok := r.(sentinel); !ok || s.msg != "boom" {
				t.Fatalf("recovered %v, want the original panic value", r)
			}
		}()
		_ = Tx(t.Context(), db, nil, func(*sql.Tx) error { panic(sentinel{"boom"}) })
		t.Fatal("Tx returned after panic")
	}()
	if !c.st.has("rollback") || c.st.has("commit") {
		t.Fatalf("events = %v, want a rollback and no commit", c.st.events())
	}
	// The connection went back to the pool and is usable.
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTxBeginError(t *testing.T) {
	db, _, _ := instrumented(t, newFullConnector())
	_ = db.Close()
	err := Tx(t.Context(), db, nil, func(*sql.Tx) error {
		t.Fatal("fn called")
		return nil
	})
	if err == nil || !strings.HasPrefix(err.Error(), "database: begin: ") {
		t.Fatalf("err = %v", err)
	}
}

func TestConcurrentOperations(t *testing.T) {
	db, tr, m := instrumented(t, newFullConnector())
	db.SetMaxOpenConns(4)
	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range perWorker {
				if err := Tx(t.Context(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(t.Context(), "INSERT")
					return err
				}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	const total = workers * perWorker
	for _, op := range []string{opBegin, opExec, opCommit} {
		if got := m.count(metricOps, "fakesql", op, "ok"); got != total {
			t.Errorf("%s ok = %v, want %d", op, got, total)
		}
	}
	if got := len(tr.snapshot()); got != 3*total {
		t.Errorf("recorded %d spans, want %d", got, 3*total)
	}
}

func BenchmarkExec(b *testing.B) {
	db := OpenDB(newFullConnector())
	defer db.Close()
	ctx := b.Context()
	for b.Loop() {
		if _, err := db.ExecContext(ctx, "INSERT"); err != nil {
			b.Fatal(err)
		}
	}
}
