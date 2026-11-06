package endpoint_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/tracing"
)

// recoveryCaptureHandler is a slog.Handler that records every record it handles.
type recoveryCaptureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recoveryCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recoveryCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recoveryCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recoveryCaptureHandler) WithGroup(string) slog.Handler      { return h }

func recoveryAttrs(r slog.Record) map[string]slog.Value {
	m := make(map[string]slog.Value)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value
		return true
	})
	return m
}

func recoveryDiscard() *recoveryLoggerFixture {
	return recoveryTestLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestRecoveryPassThrough(t *testing.T) {
	mw := endpoint.Recovery(endpoint.WithRecoveryLogger(recoveryDiscard()))
	sentinel := errors.New("boom")
	resp, err := mw(func(context.Context, any) (any, error) { return "ok", sentinel })(t.Context(), nil)
	if resp != "ok" || err != sentinel {
		t.Fatalf("got (%v, %v), want (ok, %v)", resp, err, sentinel)
	}
}

func TestRecoveryPanicBecomesInternal(t *testing.T) {
	h := &recoveryCaptureHandler{}
	mw := endpoint.Recovery(endpoint.WithRecoveryLogger(recoveryTestLogger(slog.New(h))))
	ctx := metadata.NewContext(t.Context(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /x",
	})
	ctx = tracing.ContextWithSpan(ctx, recoveryCorrelationSpan{})
	resp, err := mw(func(context.Context, any) (any, error) { panic("kaboom") })(ctx, nil)
	if resp != nil {
		t.Fatalf("resp = %v, want nil", resp)
	}
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok {
		t.Fatalf("err = %v, want *errorx.Error", err)
	}
	if e.Code != errorx.Internal || e.Reason != endpoint.ReasonPanic {
		t.Fatalf("code/reason = %v/%q", e.Code, e.Reason)
	}
	if errorx.IsRetryable(err) {
		t.Fatal("panic error must not be retryable")
	}
	if strings.Contains(e.Message, "kaboom") {
		t.Fatalf("message leaks panic value: %q", e.Message)
	}
	if !strings.Contains(err.Error(), "recovery: panic: kaboom") {
		t.Fatalf("cause missing panic value: %v", err)
	}
	if len(h.records) != 1 {
		t.Fatalf("got %d log records, want 1", len(h.records))
	}
	r := h.records[0]
	if r.Level != slog.LevelError {
		t.Fatalf("level = %v, want ERROR", r.Level)
	}
	a := recoveryAttrs(r)
	for _, key := range []string{"panic", "stack", "error"} {
		if _, ok := a[key]; ok {
			t.Fatalf("default log includes %s", key)
		}
	}
	if a["code"].String() != "Internal" || a["reason"].String() != endpoint.ReasonPanic || a["retryable"].Bool() || a["remote"].Bool() {
		t.Fatalf("classification fields: %v", a)
	}
	if a["trace_id"].String() != recoveryTraceID || a["span_id"].String() != recoverySpanID {
		t.Fatalf("trace correlation: %v", a)
	}
	for _, value := range a {
		if strings.Contains(value.String(), "kaboom") {
			t.Fatalf("panic payload leaked: %v", a)
		}
	}
	if a["operation"].String() != "GET /x" || a["protocol"].String() != "http" || a["kind"].String() != "server" {
		t.Fatalf("info attrs = %v", a)
	}
}

func TestRecoveryPanicWithErrorKeepsCause(t *testing.T) {
	sentinel := errors.New("sentinel")
	mw := endpoint.Recovery(endpoint.WithRecoveryLogger(recoveryDiscard()))
	_, err := mw(func(context.Context, any) (any, error) { panic(sentinel) })(t.Context(), nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap the panic error", err)
	}
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("code = %v", errorx.CodeOf(err))
	}
}

func TestRecoveryPanicNil(t *testing.T) {
	mw := endpoint.Recovery(endpoint.WithRecoveryLogger(recoveryDiscard()))
	_, err := mw(func(context.Context, any) (any, error) { panic(nil) })(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("panic(nil) err = %v, want Internal", err)
	}
}

func TestRecoveryHandler(t *testing.T) {
	custom := errorx.New(errorx.Unavailable, "CUSTOM", "custom")
	var got any
	sink := &recoveryCaptureHandler{}
	mw := endpoint.Recovery(
		endpoint.WithRecoveryLogger(recoveryTestLogger(slog.New(sink))),
		endpoint.WithRecoveryHandler(func(_ context.Context, p any) error {
			got = p
			if len(sink.records) != 1 {
				t.Fatal("handler ran before panic was logged")
			}
			fields := recoveryAttrs(sink.records[0])
			if fields["code"].String() != "Internal" || fields["reason"].String() != endpoint.ReasonPanic {
				t.Fatalf("panic classification: %v", fields)
			}
			return custom
		}),
	)
	_, err := mw(func(context.Context, any) (any, error) { panic(42) })(t.Context(), nil)
	if err != custom {
		t.Fatalf("err = %v, want custom", err)
	}
	if got != 42 {
		t.Fatalf("handler got %v, want 42", got)
	}
}

func TestRecoveryHandlerNilFallsBack(t *testing.T) {
	mw := endpoint.Recovery(
		endpoint.WithRecoveryLogger(recoveryDiscard()),
		endpoint.WithRecoveryHandler(func(context.Context, any) error { return nil }),
	)
	_, err := mw(func(context.Context, any) (any, error) { panic("x") })(t.Context(), nil)
	if e := errorx.From(err); e == nil || e.Reason != endpoint.ReasonPanic {
		t.Fatalf("err = %v, want default panic error", err)
	}
}

func TestRecoveryNilLoggerStillRecovers(t *testing.T) {
	mw := endpoint.Recovery(nil, endpoint.WithRecoveryLogger(nil))
	_, err := mw(func(context.Context, any) (any, error) { panic("disabled log") })(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("panic was not recovered: %v", err)
	}
}

const (
	recoveryTraceID = "0123456789abcdef0123456789abcdef"
	recoverySpanID  = "0123456789abcdef"
)

type recoveryCorrelationSpan struct{}

func (recoveryCorrelationSpan) End()                     {}
func (recoveryCorrelationSpan) SetAttribute(string, any) {}
func (recoveryCorrelationSpan) RecordError(error)        {}
func (recoveryCorrelationSpan) TraceID() string          { return recoveryTraceID }
func (recoveryCorrelationSpan) SpanID() string           { return recoverySpanID }

func TestRecoveryPanicDetailsRequireExplicitOption(t *testing.T) {
	sink := &recoveryCaptureHandler{}
	next := endpoint.Recovery(endpoint.WithRecoveryLogger(recoveryTestLogger(slog.New(sink))), endpoint.WithPanicDetails())(
		func(context.Context, any) (any, error) { panic("diagnostic-only") })
	_, err := next(t.Context(), nil)
	if errorx.Classify(err).Code != errorx.Internal {
		t.Fatal("panic was not recovered")
	}
	if len(sink.records) != 1 {
		t.Fatalf("records=%d", len(sink.records))
	}
	fields := recoveryAttrs(sink.records[0])
	if fields["panic"].String() != "diagnostic-only" {
		t.Fatal("explicit diagnostics omitted panic")
	}
	if !strings.Contains(fields["stack"].String(), "recovery_test.go") {
		t.Fatal("explicit diagnostics omitted stack")
	}
	if fields["reason"].String() != endpoint.ReasonPanic {
		t.Fatal("explicit diagnostics lost classification")
	}
}

type recoveryDisabledLogger struct{ called bool }

func (*recoveryDisabledLogger) Enabled(context.Context, logger.Level) bool          { return false }
func (l *recoveryDisabledLogger) Log(context.Context, logger.Level, string, ...any) { l.called = true }

func TestRecoveryDisabledLoggerStillRecovers(t *testing.T) {
	sink := &recoveryDisabledLogger{}
	next := endpoint.Recovery(endpoint.WithRecoveryLogger(sink), endpoint.WithPanicDetails())(
		func(context.Context, any) (any, error) { panic("private") })
	_, err := next(t.Context(), nil)
	if errorx.Classify(err).Reason != endpoint.ReasonPanic || sink.called {
		t.Fatalf("err=%v logged=%v", err, sink.called)
	}
}
