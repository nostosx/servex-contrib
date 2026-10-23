package recovery_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/recovery"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// captureHandler is a slog.Handler that records every record it handles.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func attrs(r slog.Record) map[string]slog.Value {
	m := make(map[string]slog.Value)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value
		return true
	})
	return m
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPassThrough(t *testing.T) {
	mw := recovery.New(recovery.WithLogger(discard()))
	sentinel := errors.New("boom")
	resp, err := mw(func(context.Context, any) (any, error) { return "ok", sentinel })(t.Context(), nil)
	if resp != "ok" || err != sentinel {
		t.Fatalf("got (%v, %v), want (ok, %v)", resp, err, sentinel)
	}
}

func TestPanicBecomesInternal(t *testing.T) {
	h := &captureHandler{}
	mw := recovery.New(recovery.WithLogger(slog.New(h)))
	ctx := metadata.NewContext(t.Context(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /x",
	})
	resp, err := mw(func(context.Context, any) (any, error) { panic("kaboom") })(ctx, nil)
	if resp != nil {
		t.Fatalf("resp = %v, want nil", resp)
	}
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok {
		t.Fatalf("err = %v, want *errorx.Error", err)
	}
	if e.Code != errorx.Internal || e.Reason != recovery.ReasonPanic {
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
	a := attrs(r)
	if got := a["panic"].String(); got != "kaboom" {
		t.Fatalf("panic attr = %q", got)
	}
	if got := a["stack"].String(); !strings.Contains(got, "recovery_test.go") {
		t.Fatalf("stack attr does not point at the panic site: %q", got)
	}
	if a["operation"].String() != "GET /x" || a["protocol"].String() != "http" || a["kind"].String() != "server" {
		t.Fatalf("info attrs = %v", a)
	}
}

func TestPanicWithErrorKeepsCause(t *testing.T) {
	sentinel := errors.New("sentinel")
	mw := recovery.New(recovery.WithLogger(discard()))
	_, err := mw(func(context.Context, any) (any, error) { panic(sentinel) })(t.Context(), nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap the panic error", err)
	}
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("code = %v", errorx.CodeOf(err))
	}
}

func TestPanicNil(t *testing.T) {
	mw := recovery.New(recovery.WithLogger(discard()))
	_, err := mw(func(context.Context, any) (any, error) { panic(nil) })(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("panic(nil) err = %v, want Internal", err)
	}
}

func TestHandler(t *testing.T) {
	custom := errorx.New(errorx.Unavailable, "CUSTOM", "custom")
	var got any
	mw := recovery.New(
		recovery.WithLogger(discard()),
		recovery.WithHandler(func(_ context.Context, p any) error {
			got = p
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

func TestHandlerNilFallsBack(t *testing.T) {
	mw := recovery.New(
		recovery.WithLogger(discard()),
		recovery.WithHandler(func(context.Context, any) error { return nil }),
	)
	_, err := mw(func(context.Context, any) (any, error) { panic("x") })(t.Context(), nil)
	if e := errorx.From(err); e == nil || e.Reason != recovery.ReasonPanic {
		t.Fatalf("err = %v, want default panic error", err)
	}
}

func TestDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	mw := recovery.New(nil, recovery.WithLogger(nil))
	_, _ = mw(func(context.Context, any) (any, error) { panic("default") })(t.Context(), nil)
	if !strings.Contains(buf.String(), "panic=default") {
		t.Fatalf("default logger did not record panic: %q", buf.String())
	}
}
