package gorm_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	sdkgorm "gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/nostosx/servex-contrib/database/gorm"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

type recorder struct {
	mu      sync.Mutex
	spans   []*span
	counts  map[string]float64
	samples int
}

type span struct {
	parent context.Context
	name   string
	attrs  map[string]any
	errs   []error
	ends   int
}

func newRecorder() *recorder { return &recorder{counts: make(map[string]float64)} }

func (r *recorder) Start(ctx context.Context, name string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	s := &span{parent: ctx, name: name, attrs: make(map[string]any)}
	r.mu.Lock()
	r.spans = append(r.spans, s)
	r.mu.Unlock()
	return tracing.ContextWithSpan(ctx, s), s
}

func (s *span) End()                               { s.ends++ }
func (s *span) SetAttribute(key string, value any) { s.attrs[key] = value }
func (s *span) RecordError(err error)              { s.errs = append(s.errs, err) }
func (*span) TraceID() string                      { return "" }
func (*span) SpanID() string                       { return "" }

func (r *recorder) Counter(string, string, ...string) metrics.Counter { return counter{r: r} }
func (r *recorder) Histogram(string, string, []float64, ...string) metrics.Histogram {
	return histogram{r}
}
func (*recorder) Gauge(string, string, ...string) metrics.Gauge { panic("unused") }

type counter struct {
	r      *recorder
	labels string
}

func (c counter) With(labels ...string) metrics.Counter {
	c.labels = fmt.Sprint(labels)
	return c
}
func (c counter) Add(_ context.Context, value float64) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.r.counts[c.labels] += value
}

type histogram struct{ r *recorder }

func (h histogram) With(...string) metrics.Histogram { return h }
func (h histogram) Observe(context.Context, float64) {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	h.r.samples++
}

func database(t *testing.T) (*sdkgorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sdkgorm.Open(postgres.New(postgres.Config{Conn: pool}), &sdkgorm.Config{
		DisableAutomaticPing: true, SkipDefaultTransaction: true, Logger: logger.Discard,
	})
	if err != nil {
		_ = pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = pool.Close()
	})
	return db, mock
}

type user struct {
	ID   int
	Name string
}

func TestPluginOperations(t *testing.T) {
	db, mock := database(t)
	rec := newRecorder()
	if err := db.Use(gorm.NewPlugin(rec, rec)); err != nil {
		t.Fatal(err)
	}
	parentCtx, parent := rec.Start(t.Context(), "parent", tracing.SpanKindInternal)
	ctx := parentCtx
	for _, operation := range []string{"create", "query", "update", "delete", "row", "raw"} {
		switch operation {
		case "create":
			mock.ExpectQuery("INSERT INTO").WithArgs("private name").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			if err := db.WithContext(ctx).Create(&user{Name: "private name"}).Error; err != nil {
				t.Fatal(err)
			}
		case "query":
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "private name"))
			var users []user
			result := db.WithContext(ctx).Find(&users)
			if result.Error != nil || result.Statement.Context != ctx || len(users) != 1 {
				t.Fatalf("query/context: %v, %v", users, result.Error)
			}
		case "update":
			mock.ExpectExec("UPDATE").WithArgs("new", 1).WillReturnResult(sqlmock.NewResult(0, 1))
			if err := db.WithContext(ctx).Model(&user{ID: 1}).Update("name", "new").Error; err != nil {
				t.Fatal(err)
			}
		case "delete":
			mock.ExpectExec("DELETE").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
			if err := db.WithContext(ctx).Delete(&user{ID: 1}).Error; err != nil {
				t.Fatal(err)
			}
		case "row":
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			rows, err := db.WithContext(ctx).Raw("SELECT 1").Rows()
			if err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		case "raw":
			mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
			if err := db.WithContext(ctx).Exec("UPDATE users SET name = ?", "secret").Error; err != nil {
				t.Fatal(err)
			}
		}
		if rec.counts[fmt.Sprint([]string{"postgres", operation, "ok"})] != 1 {
			t.Fatalf("missing %s metric: %v", operation, rec.counts)
		}
	}
	parent.End()
	if len(rec.spans) != 7 || rec.samples != 6 {
		t.Fatalf("spans=%d durations=%d", len(rec.spans), rec.samples)
	}
	for _, s := range rec.spans[1:] {
		if s.ends != 1 || tracing.SpanFromContext(s.parent) != parent || len(s.attrs) != 2 || len(s.errs) != 0 {
			t.Fatalf("unexpected span: %+v", s)
		}
	}
}

func TestErrorsDryRunAndCancellation(t *testing.T) {
	db, mock := database(t)
	rec := newRecorder()
	if err := db.Use(gorm.NewPlugin(rec, rec)); err != nil {
		t.Fatal(err)
	}
	errPrivate := errors.New("private SQL, parameters and credentials")
	mock.ExpectQuery("SELECT").WillReturnError(errPrivate)
	if err := db.WithContext(t.Context()).First(&user{}).Error; !errors.Is(err, errPrivate) {
		t.Fatalf("original error identity lost: %v", err)
	}
	if len(rec.spans[0].errs) != 1 || rec.spans[0].errs[0].Error() != "gorm operation failed" {
		t.Fatalf("unsafe error telemetry: %v", rec.spans[0].errs)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
	if err := db.WithContext(t.Context()).First(&user{}).Error; !errors.Is(err, sdkgorm.ErrRecordNotFound) {
		t.Fatal(err)
	}
	if len(rec.spans[1].errs) != 0 || rec.counts["[postgres query ok]"] != 1 {
		t.Fatal("record-not-found classified as backend failure")
	}
	if err := db.WithContext(t.Context()).Session(&sdkgorm.Session{DryRun: true}).Find(&[]user{}).Error; err != nil {
		t.Fatal(err)
	}
	if len(rec.spans) != 2 {
		t.Fatal("DryRun recorded as real database operation")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := db.WithContext(ctx).Find(&[]user{}).Error; !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation lost: %v", err)
	}
}

func TestConcurrentQueries(t *testing.T) {
	db, mock := database(t)
	mock.MatchExpectationsInOrder(false)
	rec := newRecorder()
	if err := db.Use(gorm.NewPlugin(rec, rec)); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "alice"))
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := db.WithContext(t.Context()).Find(&[]user{}).Error; err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(rec.spans) != 20 || rec.counts["[postgres query ok]"] != 20 {
		t.Fatalf("concurrent operations: %d, %v", len(rec.spans), rec.counts)
	}
	for _, s := range rec.spans {
		if s.ends != 1 || tracing.SpanFromContext(s.parent) != nil {
			t.Fatalf("request state leaked: %+v", s)
		}
	}
}

func TestHookLifecycle(t *testing.T) {
	db, mock := database(t)
	hook, err := gorm.Hook("postgres", db)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()
	if err := hook.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()
	if err := hook.(health.Checker).Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	errPing := errors.New("backend unavailable")
	mock.ExpectPing().WillReturnError(errPing)
	if err := hook.(health.Checker).Check(t.Context()); !errors.Is(err, errPing) {
		t.Fatalf("readiness error: %v", err)
	}
	mock.ExpectClose()
	if err := hook.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := hook.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := gorm.Hook("invalid", nil); err == nil {
		t.Fatal("accepted nil database")
	}
}

func TestNilSignalsAndDuplicatePlugin(t *testing.T) {
	db, mock := database(t)
	plugin := gorm.NewPlugin(nil, nil)
	if err := plugin.Initialize(nil); err == nil {
		t.Fatal("accepted nil database")
	}
	if err := db.Use(plugin); err != nil {
		t.Fatal(err)
	}
	if err := db.Use(gorm.NewPlugin(nil, nil)); !errors.Is(err, sdkgorm.ErrRegistered) {
		t.Fatalf("duplicate plugin: %v", err)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	if err := db.WithContext(t.Context()).Find(&[]user{}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestHookRejectsTransaction(t *testing.T) {
	db, mock := database(t)
	mock.ExpectBegin()
	tx := db.WithContext(t.Context()).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if _, err := gorm.Hook("transaction", tx); err == nil {
		t.Fatal("transaction was accepted as pool lifecycle owner")
	}
	mock.ExpectRollback()
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
}
