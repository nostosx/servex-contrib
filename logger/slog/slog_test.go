package slog_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"sync"
	"testing"

	adapter "github.com/nostosx/servex-contrib/logger/slog"
	"github.com/nostosx/servex/logger"
)

func TestStructuredFieldsAndFiltering(t *testing.T) {
	var output bytes.Buffer
	backend := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelWarn}))
	var log logger.Logger = adapter.New(backend.With("service", "orders"))
	if log.Enabled(t.Context(), logger.LevelInfo) || !log.Enabled(t.Context(), logger.LevelError) {
		t.Fatal("backend level filter was lost")
	}
	log.Log(t.Context(), logger.LevelInfo, "hidden")
	log.Log(t.Context(), logger.LevelError, "failed", "order_id", 42)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["service"] != "orders" || record["order_id"] != float64(42) || record["msg"] != "failed" || record["level"] != "ERROR" {
		t.Fatalf("record=%v", record)
	}
}

type contextKey struct{}
type contextHandler struct{ seen context.Context }

func (*contextHandler) Enabled(context.Context, slog.Level) bool          { return true }
func (h *contextHandler) Handle(ctx context.Context, _ slog.Record) error { h.seen = ctx; return nil }
func (h *contextHandler) WithAttrs([]slog.Attr) slog.Handler              { return h }
func (h *contextHandler) WithGroup(string) slog.Handler                   { return h }

func TestContextAndConcurrentUse(t *testing.T) {
	handler := &contextHandler{}
	log := adapter.New(slog.New(handler))
	ctx := context.WithValue(t.Context(), contextKey{}, "request")
	log.Log(ctx, logger.LevelInfo, "request")
	if handler.seen != ctx {
		t.Fatal("context was not passed to handler")
	}
	// Use a concurrency-safe standard handler for parallel emission.
	log = adapter.New(slog.New(slog.DiscardHandler))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				log.Log(ctx, logger.LevelDebug, "parallel", "n", 1)
			}
		})
	}
	wg.Wait()
}

func TestNilCapturesDefaultWithoutChangingIt(t *testing.T) {
	original := slog.Default()
	_ = adapter.New(nil)
	if slog.Default() != original {
		t.Fatal("constructor changed process logger")
	}
}
