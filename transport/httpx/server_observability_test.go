package httpx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/tracing"
)

type observedRecord struct {
	ctx     context.Context
	level   logger.Level
	message string
	fields  map[string]any
}

type recordingLogger struct {
	enabled bool
	checks  int
	records []observedRecord
}

func (l *recordingLogger) Enabled(context.Context, logger.Level) bool {
	l.checks++
	return l.enabled
}

func (l *recordingLogger) Log(ctx context.Context, level logger.Level, message string, fields ...any) {
	record := observedRecord{ctx: ctx, level: level, message: message, fields: make(map[string]any)}
	for i := 0; i < len(fields); i += 2 {
		record.fields[fields[i].(string)] = fields[i+1]
	}
	l.records = append(l.records, record)
}

type observedSpan struct{ traceID, spanID string }

func (observedSpan) End()                     {}
func (observedSpan) SetAttribute(string, any) {}
func (observedSpan) RecordError(error)        {}
func (s observedSpan) TraceID() string        { return s.traceID }
func (s observedSpan) SpanID() string         { return s.spanID }

func TestServerErrorLogClassifiesWithoutChangingResponse(t *testing.T) {
	t.Parallel()
	log := &recordingLogger{enabled: true}
	private := errorx.New(errorx.Internal, "ORDER_FAILED", "private message").
		WithMetadata(map[string]string{"token": "private metadata"}).
		WithCause(errors.New("private cause")).WithRemote().WithRetryable(true)
	s := NewServer(WithLogger(log))
	s.Handle("GET /orders/{id}", func(context.Context, any) (any, error) { return nil, private }, nil)
	ctx := tracing.ContextWithSpan(t.Context(), observedSpan{traceID: "backend-trace", spanID: "backend-span"})
	r := httptest.NewRequest(http.MethodGet, "/orders/private-path?token=private-query", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), `"message":"private message"`) || !strings.Contains(w.Body.String(), `"token":"private metadata"`) {
		t.Fatalf("response changed: status=%d body=%s", w.Code, w.Body)
	}
	if log.checks != 1 || len(log.records) != 1 {
		t.Fatalf("checks=%d records=%d, want one enabled check and one status log", log.checks, len(log.records))
	}
	record := log.records[0]
	want := map[string]any{
		"operation": "GET /orders/{id}", "status": http.StatusInternalServerError,
		"code": "Internal", "reason": "ORDER_FAILED", "retryable": true, "remote": true,
		"trace_id": "backend-trace", "span_id": "backend-span",
	}
	if record.ctx != ctx || record.level != logger.LevelError || record.message != "httpx: request failed" {
		t.Fatalf("unexpected status log: %+v", record)
	}
	for key, value := range want {
		if record.fields[key] != value {
			t.Errorf("field %q=%v, want %v", key, record.fields[key], value)
		}
	}
	if len(record.fields) != len(want) || strings.Contains(fmt.Sprint(record), "private") {
		t.Errorf("log contains unexpected or private data: %+v", record)
	}
}

func TestServerReadinessLogUsesSafeClassification(t *testing.T) {
	t.Parallel()
	log := &recordingLogger{enabled: true}
	err := errorx.New(errorx.Unavailable, "DATABASE_NOT_READY", "private DSN").WithCause(errors.New("private credential"))
	s := NewServer(WithLogger(log), WithHealth(health.CheckerFunc(func(context.Context) error { return err })))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable || w.Body.String() != "not ready\n" || len(log.records) != 1 {
		t.Fatalf("status=%d body=%q logs=%d", w.Code, w.Body, len(log.records))
	}
	record := log.records[0]
	if record.level != logger.LevelWarn || record.fields["code"] != "Unavailable" || record.fields["reason"] != "DATABASE_NOT_READY" || record.fields["retryable"] != true || record.fields["remote"] != false || strings.Contains(fmt.Sprint(record), "private") {
		t.Errorf("unexpected readiness log: %+v", record)
	}
}

func TestServerLogOmitsIncompleteSpanIDs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		span tracing.Span
	}{
		{"absent", nil},
		{"empty trace", observedSpan{spanID: "span"}},
		{"empty span", observedSpan{traceID: "trace"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &recordingLogger{enabled: true}
			s := NewServer(WithLogger(log))
			s.logError(tracing.ContextWithSpan(t.Context(), tc.span), logger.LevelError, "failed", errors.New("private message"))
			fields := log.records[0].fields
			if _, ok := fields["reason"]; ok {
				t.Error("unstructured error produced an empty reason field")
			}
			if _, ok := fields["trace_id"]; ok {
				t.Error("incomplete span produced a trace ID")
			}
			if _, ok := fields["span_id"]; ok {
				t.Error("incomplete span produced a span ID")
			}
		})
	}
}

func TestServerDisabledLoggerSkipsErrorClassification(t *testing.T) {
	t.Parallel()
	log := &recordingLogger{}
	s := NewServer(WithLogger(log))
	probe := &classificationProbe{}
	s.logError(t.Context(), logger.LevelError, "failed", probe)
	if log.checks != 1 || len(log.records) != 0 || probe.inspected {
		t.Fatalf("disabled logger performed work: checks=%d records=%d inspected=%v", log.checks, len(log.records), probe.inspected)
	}
}

type classificationProbe struct{ inspected bool }

func (*classificationProbe) Error() string { panic("raw error must not be formatted") }
func (p *classificationProbe) As(any) bool {
	p.inspected = true
	return false
}

func TestHTTPDiagnosticWriterNeverLogsNativeText(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			log := &recordingLogger{enabled: enabled}
			writer := errorLogWriter{server: NewServer(WithLogger(log))}
			text := []byte("http: panic serving private-address: private-token\nprivate stack\n")
			n, err := writer.Write(text)
			if n != len(text) || err != nil || log.checks != 1 {
				t.Fatalf("Write=%d,%v checks=%d", n, err, log.checks)
			}
			if !enabled {
				if len(log.records) != 0 {
					t.Fatal("disabled logger received a diagnostic")
				}
				return
			}
			if len(log.records) != 1 {
				t.Fatalf("diagnostic logs=%d, want one", len(log.records))
			}
			record := log.records[0]
			if record.message != "httpx: server diagnostic" || record.fields["code"] != "Unknown" || record.fields["reason"] != "HTTP_SERVER_DIAGNOSTIC" || strings.Contains(fmt.Sprint(record), "private") {
				t.Errorf("unsafe diagnostic: %+v", record)
			}
		})
	}
}

type failedListener struct{ err error }

func (l failedListener) Accept() (net.Conn, error) { return nil, l.err }
func (failedListener) Close() error                { return nil }
func (failedListener) Addr() net.Addr              { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080} }

func TestServerServeLogPreservesRuntimeErrorWithoutLoggingCause(t *testing.T) {
	t.Parallel()
	log := &recordingLogger{enabled: true}
	s := NewServer(WithLogger(log))
	wantErr := errors.New("private listener failure")
	s.serve(&http.Server{}, failedListener{err: wantErr})
	if err := <-s.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("runtime error=%v, want original cause", err)
	}
	if len(log.records) != 1 || log.records[0].fields["code"] != "Unknown" || strings.Contains(fmt.Sprint(log.records), "private") {
		t.Errorf("unsafe serve log: %+v", log.records)
	}
}
