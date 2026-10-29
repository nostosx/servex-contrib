package zap_test

import (
	"sync"
	"testing"

	upstream "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	adapter "github.com/nostosx/servex-contrib/logger/zap"
	"github.com/nostosx/servex/logger"
)

func TestLevelMappingFieldsAndFiltering(t *testing.T) {
	core, records := observer.New(zapcore.WarnLevel)
	var log logger.Logger = adapter.New(upstream.New(core).With(upstream.String("service", "orders")))
	if log.Enabled(t.Context(), logger.LevelInfo) || !log.Enabled(t.Context(), logger.LevelWarn) {
		t.Fatal("backend filter was lost")
	}
	log.Log(t.Context(), logger.LevelInfo, "hidden")
	log.Log(t.Context(), logger.LevelWarn, "warning", "order_id", 42)
	// Even very high levels must never become Zap's panic/fatal levels.
	log.Log(t.Context(), logger.Level(100), "error", "reason", "failure")
	got := records.All()
	if len(got) != 2 || got[0].Level != zapcore.WarnLevel || got[1].Level != zapcore.ErrorLevel {
		t.Fatalf("entries=%v", got)
	}
	fields := got[0].ContextMap()
	if fields["service"] != "orders" || fields["order_id"] != int64(42) {
		t.Fatalf("fields=%v", fields)
	}
}

func TestConcurrentUseAndNil(t *testing.T) {
	log := adapter.New(nil)
	if log.Enabled(t.Context(), logger.LevelError) {
		t.Fatal("nil backend must be no-op")
	}
	core, records := observer.New(zapcore.DebugLevel)
	log = adapter.New(upstream.New(core))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				log.Log(t.Context(), logger.LevelInfo, "parallel", "n", 1)
			}
		})
	}
	wg.Wait()
	if records.Len() != 800 {
		t.Fatalf("records=%d", records.Len())
	}
}
