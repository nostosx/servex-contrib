package logging_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/logging"
	"github.com/nostosx/servex/errorx"
	sxlog "github.com/nostosx/servex/logger"
	"github.com/nostosx/servex/metadata"
)

// captureHandler records every record at or above its minimum level.
type captureHandler struct {
	min     slog.Level
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.min }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) only(t *testing.T) (slog.Record, map[string]slog.Value) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("got %d records, want 1", len(h.records))
	}
	r := h.records[0]
	m := make(map[string]slog.Value)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value
		return true
	})
	return r, m
}

func serverCtx(ctx context.Context) context.Context {
	return metadata.NewContext(ctx, metadata.Info{
		Kind: metadata.KindServer, Protocol: "grpc", Operation: "/user.v1.Users/Get",
	})
}

func TestSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &captureHandler{min: slog.LevelDebug}
		ep := logging.New(testLogger(slog.New(h)))(func(context.Context, any) (any, error) {
			time.Sleep(150 * time.Millisecond)
			return "resp", nil
		})
		resp, err := ep(serverCtx(t.Context()), nil)
		if err != nil || resp != "resp" {
			t.Fatalf("got (%v, %v)", resp, err)
		}
		r, a := h.only(t)
		if r.Level != slog.LevelInfo || r.Message != logging.Message {
			t.Fatalf("record = %v %q", r.Level, r.Message)
		}
		want := map[string]string{
			logging.KeyKind:      "server",
			logging.KeyProtocol:  "grpc",
			logging.KeyOperation: "/user.v1.Users/Get",
			logging.KeyCode:      "OK",
		}
		for k, v := range want {
			if got := a[k].String(); got != v {
				t.Errorf("%s = %q, want %q", k, got, v)
			}
		}
		if d := a[logging.KeyDuration]; d.Kind() != slog.KindDuration || d.Duration() != 150*time.Millisecond {
			t.Errorf("duration = %v", d)
		}
		if _, ok := a[logging.KeyError]; ok {
			t.Error("error attribute on success")
		}
		if _, ok := a[logging.KeyReason]; ok {
			t.Error("reason attribute on success")
		}
	})
}

func TestFailureLevels(t *testing.T) {
	cases := []struct {
		err   error
		level slog.Level
		code  string
	}{
		{errorx.New(errorx.NotFound, "USER_NOT_FOUND", "no user"), slog.LevelWarn, "NotFound"},
		{errorx.New(errorx.InvalidArgument, "", "bad"), slog.LevelWarn, "InvalidArgument"},
		{errorx.New(errorx.Unauthenticated, "", ""), slog.LevelWarn, "Unauthenticated"},
		{context.Canceled, slog.LevelWarn, "Canceled"},
		{errorx.New(errorx.Internal, "PANIC", "x"), slog.LevelError, "Internal"},
		{errorx.New(errorx.Unavailable, "", ""), slog.LevelError, "Unavailable"},
		{context.DeadlineExceeded, slog.LevelError, "DeadlineExceeded"},
		{errors.New("plain"), slog.LevelError, "Unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			h := &captureHandler{min: slog.LevelDebug}
			ep := logging.New(testLogger(slog.New(h)))(func(context.Context, any) (any, error) { return nil, tc.err })
			if _, err := ep(serverCtx(t.Context()), nil); err != tc.err {
				t.Fatalf("err = %v, want unchanged %v", err, tc.err)
			}
			r, a := h.only(t)
			if r.Level != tc.level {
				t.Fatalf("level = %v, want %v", r.Level, tc.level)
			}
			if a[logging.KeyCode].String() != tc.code {
				t.Fatalf("code = %v", a[logging.KeyCode])
			}
			if a[logging.KeyError].String() != tc.err.Error() {
				t.Fatalf("error = %v", a[logging.KeyError])
			}
			if reason := errorx.From(tc.err).Reason; reason != "" && a[logging.KeyReason].String() != reason {
				t.Fatalf("reason = %v", a[logging.KeyReason])
			}
		})
	}
}

func TestDefaultLevelUnknownCode(t *testing.T) {
	if l := logging.DefaultLevel(errorx.Code(99)); l != sxlog.LevelError {
		t.Fatalf("level = %v", l)
	}
}

func TestCustomLevelAndDisabled(t *testing.T) {
	h := &captureHandler{min: slog.LevelInfo}
	mw := logging.New(testLogger(slog.New(h)), nil, logging.WithLevel(nil), logging.WithLevel(func(code errorx.Code) sxlog.Level {
		if code == errorx.OK {
			return sxlog.LevelDebug // below the handler's minimum
		}
		return sxlog.LevelError
	}))
	if _, err := mw(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if len(h.records) != 0 {
		t.Fatalf("disabled level logged %d records", len(h.records))
	}
	_, _ = mw(func(context.Context, any) (any, error) { return nil, errorx.New(errorx.NotFound, "", "") })(t.Context(), nil)
	r, a := h.only(t)
	if r.Level != slog.LevelError {
		t.Fatalf("level = %v", r.Level)
	}
	if a[logging.KeyKind].String() != "" || a[logging.KeyOperation].String() != "" {
		t.Fatalf("missing info should log empty values: %v", a)
	}
}

func TestNilLoggerDisablesLogging(t *testing.T) {
	called := false
	_, err := logging.New(nil)(func(context.Context, any) (any, error) { called = true; return "ok", nil })(t.Context(), nil)
	if !called || err != nil {
		t.Fatalf("nil logger changed execution: %v", err)
	}
}
