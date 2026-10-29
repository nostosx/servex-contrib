package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	noopmetrics "github.com/nostosx/servex-contrib/metrics/noop"

	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// Names under which the fake drivers are registered for the test binary.
const (
	ctxDriverName    = "servex-database-fake-ctx"
	legacyDriverName = "servex-database-fake-legacy"
)

func TestMain(m *testing.M) {
	sql.Register(ctxDriverName, ctxDriver{})
	sql.Register(legacyDriverName, legacyDriver{})
	m.Run()
}

var (
	errFake        = errors.New("fake: failure")
	errStmtChecker = errors.New("fake: statement checker")
)

// fakeState is shared by the connections of one fake database and records
// the calls that reach the driver.
type fakeState struct {
	dsn string

	mu          sync.Mutex
	log         []string
	pingErr     error
	commitErr   error
	rollbackErr error
	sleep       time.Duration // added to every exec and query
}

func (s *fakeState) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, event)
}

func (s *fakeState) events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

func (s *fakeState) has(event string) bool { return slices.Contains(s.events(), event) }

func (s *fakeState) set(f func(s *fakeState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

// stmtKind selects the optional interfaces of the statements of a fakeConn.
type stmtKind int

const (
	stmtLegacy stmtKind = iota // driver.Stmt only
	stmtCtx                    // plus StmtExecContext and StmtQueryContext
	stmtCC                     // plus ColumnConverter
	stmtNVC                    // plus NamedValueChecker
)

// fakeConn implements only driver.Conn. Its optional interfaces live on the
// named types below and are attached with anonymous structs.
type fakeConn struct {
	st           *fakeState
	stmts        stmtKind
	viaConnector bool
}

type (
	fakeExecerCtx  fakeConn
	fakeExecer     fakeConn
	fakeQueryerCtx fakeConn
	fakeQueryer    fakeConn
	fakePrepareCtx fakeConn
	fakeBeginTx    fakeConn
	fakePinger     fakeConn
	fakeResetter   fakeConn
	fakeValidator  fakeConn
	fakeConnNVC    fakeConn
)

// fullConn returns c with every context-aware optional interface.
func fullConn(c *fakeConn) driver.Conn {
	c.stmts = stmtCtx
	return struct {
		*fakeConn
		driver.ExecerContext
		driver.QueryerContext
		driver.ConnPrepareContext
		driver.ConnBeginTx
		driver.Pinger
		driver.SessionResetter
		driver.Validator
		driver.NamedValueChecker
	}{
		c, (*fakeExecerCtx)(c), (*fakeQueryerCtx)(c), (*fakePrepareCtx)(c), (*fakeBeginTx)(c),
		(*fakePinger)(c), (*fakeResetter)(c), (*fakeValidator)(c), (*fakeConnNVC)(c),
	}
}

// spanName returns the name of the recorded span carried by ctx, or "".
func spanName(ctx context.Context) string {
	if s, ok := tracing.SpanFromContext(ctx).(*recSpan); ok {
		return s.name
	}
	return ""
}

func (c *fakeConn) do(ctx context.Context, event, query string) error {
	c.st.record(event + ":" + query)
	if name := spanName(ctx); name != "" {
		c.st.record("span:" + name)
	}
	c.st.mu.Lock()
	sleep := c.st.sleep
	c.st.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
	if strings.Contains(query, "fail") {
		return errFake
	}
	return nil
}

func (c *fakeConn) exec(ctx context.Context, event, query string) (driver.Result, error) {
	if err := c.do(ctx, event, query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) query(ctx context.Context, event, query string) (driver.Rows, error) {
	if err := c.do(ctx, event, query); err != nil {
		return nil, err
	}
	return &fakeRows{values: []driver.Value{query}}, nil
}

func (c *fakeConn) newStmt(query string) driver.Stmt {
	s := &fakeStmt{c: c, query: query}
	switch c.stmts {
	case stmtCtx:
		return struct {
			*fakeStmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, (*fakeStmtExecCtx)(s), (*fakeStmtQueryCtx)(s)}
	case stmtCC:
		type converter = driver.ColumnConverter //nolint:staticcheck // fake of a legacy driver; a field named ColumnConverter would shadow the method
		return struct {
			*fakeStmt
			converter
		}{s, (*fakeStmtCC)(s)}
	case stmtNVC:
		return struct {
			*fakeStmt
			driver.NamedValueChecker
		}{s, (*fakeStmtNVC)(s)}
	default:
		return s
	}
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	if err := c.do(context.Background(), "prepare", query); err != nil {
		return nil, err
	}
	return c.newStmt(query), nil
}

func (c *fakeConn) Close() error { return nil }

// fake returns c; it is promoted through the anonymous wrapper structs.
func (c *fakeConn) fake() *fakeConn { return c }

func (c *fakeConn) Begin() (driver.Tx, error) {
	c.st.record("begin")
	return &fakeTx{st: c.st}, nil
}

func (c *fakeExecerCtx) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "skip") {
		c.st.record("skip:" + query)
		return nil, driver.ErrSkip
	}
	return (*fakeConn)(c).exec(ctx, "exec", query)
}

func (c *fakeExecer) Exec(query string, _ []driver.Value) (driver.Result, error) {
	return (*fakeConn)(c).exec(context.Background(), "legacy-exec", query)
}

func (c *fakeQueryerCtx) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return (*fakeConn)(c).query(ctx, "query", query)
}

func (c *fakeQueryer) Query(query string, _ []driver.Value) (driver.Rows, error) {
	return (*fakeConn)(c).query(context.Background(), "legacy-query", query)
}

func (c *fakePrepareCtx) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := (*fakeConn)(c).do(ctx, "prepare-ctx", query); err != nil {
		return nil, err
	}
	return (*fakeConn)(c).newStmt(query), nil
}

func (c *fakeBeginTx) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.st.record("begin-tx")
	if name := spanName(ctx); name != "" {
		c.st.record("span:" + name)
	}
	return &fakeTx{st: c.st}, nil
}

func (c *fakePinger) Ping(context.Context) error {
	c.st.record("ping")
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	return c.st.pingErr
}

func (c *fakeResetter) ResetSession(context.Context) error {
	c.st.record("reset")
	return nil
}

func (c *fakeValidator) IsValid() bool {
	c.st.record("is-valid")
	return true
}

func (c *fakeConnNVC) CheckNamedValue(*driver.NamedValue) error {
	c.st.record("conn-nvc")
	return driver.ErrSkip
}

// fakeStmt implements only driver.Stmt.
type fakeStmt struct {
	c     *fakeConn
	query string
}

type (
	fakeStmtExecCtx  fakeStmt
	fakeStmtQueryCtx fakeStmt
	fakeStmtCC       fakeStmt
	fakeStmtNVC      fakeStmt
)

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return s.c.exec(context.Background(), "stmt-exec", s.query)
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.c.query(context.Background(), "stmt-query", s.query)
}

func (s *fakeStmtExecCtx) ExecContext(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
	return s.c.exec(ctx, "stmt-exec-ctx", s.query)
}

func (s *fakeStmtQueryCtx) QueryContext(ctx context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return s.c.query(ctx, "stmt-query-ctx", s.query)
}

func (s *fakeStmtCC) ColumnConverter(int) driver.ValueConverter {
	return driver.DefaultParameterConverter
}

func (s *fakeStmtNVC) CheckNamedValue(*driver.NamedValue) error { return errStmtChecker }

// fakeRows returns a single row with a single column "v".
type fakeRows struct {
	values []driver.Value
	done   bool
}

func (r *fakeRows) Columns() []string { return []string{"v"} }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

type fakeTx struct{ st *fakeState }

func (t *fakeTx) Commit() error {
	t.st.record("commit")
	t.st.mu.Lock()
	defer t.st.mu.Unlock()
	return t.st.commitErr
}

func (t *fakeTx) Rollback() error {
	t.st.record("rollback")
	t.st.mu.Lock()
	defer t.st.mu.Unlock()
	return t.st.rollbackErr
}

// fakeConnector creates connections with newConn and records Close.
type fakeConnector struct {
	st      *fakeState
	newConn func() driver.Conn
	driver  driver.Driver
	closed  atomic.Bool
}

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.newConn(), nil }
func (c *fakeConnector) Driver() driver.Driver                        { return c.driver }

func (c *fakeConnector) Close() error {
	c.closed.Store(true)
	return nil
}

// newFullConnector returns a connector of fully featured connections.
func newFullConnector() *fakeConnector {
	st := &fakeState{}
	return &fakeConnector{
		st:      st,
		newConn: func() driver.Conn { return fullConn(&fakeConn{st: st}) },
		driver:  ctxDriver{},
	}
}

// ctxDriver implements driver.DriverContext.
type ctxDriver struct{}

func (ctxDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake: Open called on a DriverContext driver")
}

func (d ctxDriver) OpenConnector(dsn string) (driver.Connector, error) {
	if dsn == "bad" {
		return nil, errFake
	}
	st := &fakeState{dsn: dsn}
	return &fakeConnector{
		st:      st,
		newConn: func() driver.Conn { return fullConn(&fakeConn{st: st, viaConnector: true}) },
		driver:  d,
	}, nil
}

// legacyDriver implements only driver.Driver, with bare connections.
type legacyDriver struct{}

func (legacyDriver) Open(dsn string) (driver.Conn, error) {
	return &fakeConn{st: &fakeState{dsn: dsn}}, nil
}

// recTracer records spans.
type recTracer struct {
	mu    sync.Mutex
	spans []*recSpan
}

type recSpan struct {
	t      *recTracer
	name   string
	kind   tracing.SpanKind
	parent string
	attrs  map[string]any
	err    error
	ended  bool
}

func (t *recTracer) Start(ctx context.Context, name string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	s := &recSpan{t: t, name: name, kind: kind, attrs: map[string]any{}}
	if p, ok := tracing.SpanFromContext(ctx).(*recSpan); ok {
		s.parent = p.name
	}
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return tracing.ContextWithSpan(ctx, s), s
}

func (s *recSpan) End() {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	s.ended = true
}

func (s *recSpan) SetAttribute(key string, value any) {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	s.attrs[key] = value
}

func (s *recSpan) RecordError(err error) {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	s.err = err
}

func (s *recSpan) TraceID() string { return "trace" }
func (s *recSpan) SpanID() string  { return s.name }

// spanSnapshot is a comparable copy of a recorded span.
type spanSnapshot struct {
	name, parent, system, statement string
	kind                            tracing.SpanKind
	failed, ended                   bool
}

// snapshot returns the recorded spans, excluding those named in skip.
func (t *recTracer) snapshot(skip ...string) []spanSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []spanSnapshot
	for _, s := range t.spans {
		if slices.Contains(skip, s.name) {
			continue
		}
		system, _ := s.attrs[attrSystem].(string)
		statement, _ := s.attrs[attrStatement].(string)
		out = append(out, spanSnapshot{
			name: s.name, parent: s.parent, system: system, statement: statement,
			kind: s.kind, failed: s.err != nil, ended: s.ended,
		})
	}
	return out
}

// recMeter records counters and histograms by label values.
type recMeter struct {
	mu         sync.Mutex
	counters   map[string]map[string]float64
	histograms map[string]map[string][]float64
	buckets    map[string][]float64
	labels     map[string][]string
	// spans names the span found in the context of every observation.
	spans []string
}

func newRecMeter() *recMeter {
	return &recMeter{
		counters:   map[string]map[string]float64{},
		histograms: map[string]map[string][]float64{},
		buckets:    map[string][]float64{},
		labels:     map[string][]string{},
	}
}

func (m *recMeter) Counter(name, _ string, labelNames ...string) metrics.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counters[name] == nil {
		m.counters[name] = map[string]float64{}
	}
	m.labels[name] = labelNames
	return &recCounter{m: m, name: name}
}

func (m *recMeter) Gauge(string, string, ...string) metrics.Gauge {
	return noopmetrics.New().Gauge("", "")
}

func (m *recMeter) Histogram(name, _ string, buckets []float64, labelNames ...string) metrics.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.histograms[name] == nil {
		m.histograms[name] = map[string][]float64{}
	}
	m.buckets[name] = buckets
	m.labels[name] = labelNames
	return &recHistogram{m: m, name: name}
}

func (m *recMeter) count(name string, labels ...string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name][strings.Join(labels, ",")]
}

func (m *recMeter) observations(name string, labels ...string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.histograms[name][strings.Join(labels, ",")])
}

// recordSpanLocked records the name of the recorded span carried by ctx, or
// "" when there is none. m.mu must be held.
func (m *recMeter) recordSpanLocked(ctx context.Context) {
	name := ""
	if s, ok := tracing.SpanFromContext(ctx).(*recSpan); ok {
		name = s.name
	}
	m.spans = append(m.spans, name)
}

// observedSpans returns the spans recorded by recordSpanLocked.
func (m *recMeter) observedSpans() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.spans)
}

type recCounter struct {
	m      *recMeter
	name   string
	labels []string
}

func (c *recCounter) With(labelValues ...string) metrics.Counter {
	return &recCounter{m: c.m, name: c.name, labels: labelValues}
}

func (c *recCounter) Add(ctx context.Context, delta float64) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.recordSpanLocked(ctx)
	c.m.counters[c.name][strings.Join(c.labels, ",")] += delta
}

type recHistogram struct {
	m      *recMeter
	name   string
	labels []string
}

func (h *recHistogram) With(labelValues ...string) metrics.Histogram {
	return &recHistogram{m: h.m, name: h.name, labels: labelValues}
}

func (h *recHistogram) Observe(ctx context.Context, v float64) {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	h.m.recordSpanLocked(ctx)
	key := strings.Join(h.labels, ",")
	h.m.histograms[h.name][key] = append(h.m.histograms[h.name][key], v)
}
