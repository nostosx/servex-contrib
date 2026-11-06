package zap

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	upstream "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/nostosx/servex/observability/logger"
)

type borrowedWriter struct {
	bytes.Buffer
	closes, syncs int
	syncErr       error
}

func (w *borrowedWriter) Close() error { w.closes++; return nil }
func (w *borrowedWriter) Sync() error  { w.syncs++; return w.syncErr }

func TestConfigDecodedFieldsAndActualOutput(t *testing.T) {
	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"level":"warn","format":"json","output":"stderr","add_source":true}`), &cfg); err != nil {
		t.Fatal(err)
	}
	var output borrowedWriter
	log, err := New(cfg, WithWriter(&output))
	if err != nil {
		t.Fatal(err)
	}
	if log.Enabled(t.Context(), logger.LevelInfo) || !log.Enabled(t.Context(), logger.LevelWarn) {
		t.Fatal("configured level not applied")
	}
	log.Log(t.Context(), logger.LevelInfo, "hidden")
	log.Log(t.Context(), logger.LevelWarn, "visible", "order_id", 42)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "visible" || record["level"] != "warn" || record["order_id"] != float64(42) || !strings.Contains(record["source"].(string), "config_test.go") {
		t.Fatalf("record=%v", record)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if output.closes != 0 || output.syncs != 1 {
		t.Fatalf("closes=%d syncs=%d", output.closes, output.syncs)
	}
}

func TestConfigDefaultsAndText(t *testing.T) {
	for _, cfg := range []Config{{}, DefaultConfig(), {Level: " DEBUG ", Format: "text"}} {
		var output bytes.Buffer
		log, err := New(cfg, WithWriter(&output))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := log.Close(); err != nil {
				t.Error(err)
			}
		})
		log.Log(t.Context(), logger.LevelInfo, "hello")
		if cfg.Format == "text" {
			if !strings.Contains(output.String(), "info") || !strings.Contains(output.String(), "hello") {
				t.Fatalf("text=%q", output.String())
			}
		} else {
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["source"] != nil {
				t.Fatal("source unexpectedly enabled")
			}
		}
	}
}

func TestConfigRejectsBeforeOpeningOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-created.log")
	for _, cfg := range []Config{{Level: "fatal", Output: path}, {Format: "invalid", Output: path}, {Output: "bad\x00output"}} {
		if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validation=%v", err)
		}
		if log, err := New(cfg); !errors.Is(err, ErrInvalidConfig) || log != nil {
			t.Fatalf("log=%v err=%v", log, err)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("output created: %v", err)
	}
	if log, err := New(Config{Output: path}, WithWriter(nil)); !errors.Is(err, ErrInvalidConfig) || log != nil {
		t.Fatalf("log=%v err=%v", log, err)
	}
	if log, err := New(Config{Output: path}, WithWriter((*borrowedWriter)(nil))); !errors.Is(err, ErrInvalidConfig) || log != nil {
		t.Fatalf("log=%v err=%v", log, err)
	}
	if _, err := New(Config{Output: t.TempDir()}); err == nil {
		t.Fatal("directory output accepted")
	}
}

func TestOwnedOutputAppendsAndCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	for range 2 {
		log, err := New(Config{Output: path})
		if err != nil {
			t.Fatal(err)
		}
		log.Log(t.Context(), logger.LevelInfo, "hello")
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := log.output.Write([]byte("closed")); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("owned file remains open: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte("\n")) != 2 {
		t.Fatalf("data=%q", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("permissions=%v", info.Mode().Perm())
	}
}

func TestStandardAndWrappedOutputsAreBorrowed(t *testing.T) {
	for _, output := range []string{"stdout", "stderr"} {
		log, err := New(Config{Output: output})
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, output := range []*os.File{os.Stdout, os.Stderr} {
		log, err := New(DefaultConfig(), WithWriter(output))
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stdout.Stat(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stderr.Stat(); err != nil {
		t.Fatal(err)
	}
	var writer borrowedWriter
	native := upstream.New(zapcore.NewCore(zapcore.NewJSONEncoder(upstream.NewProductionEncoderConfig()), &writer, zapcore.DebugLevel))
	if err := Wrap(native).Close(); err != nil {
		t.Fatal(err)
	}
	if writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("wrapped backend ownership changed")
	}
}

func TestBorrowedWriterSyncFailurePreserved(t *testing.T) {
	want := errors.New("sync failed")
	output := borrowedWriter{syncErr: want}
	log, err := New(DefaultConfig(), WithWriter(&output))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := log.Close(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if output.closes != 0 || output.syncs != 1 {
		t.Fatalf("closes=%d syncs=%d", output.closes, output.syncs)
	}
}

func TestWrappedCallerLocation(t *testing.T) {
	var output bytes.Buffer
	native := upstream.New(zapcore.NewCore(zapcore.NewJSONEncoder(upstream.NewProductionEncoderConfig()), zapcore.AddSync(&output), zapcore.DebugLevel), upstream.AddCaller())
	log := Wrap(native)
	log.Log(t.Context(), logger.LevelInfo, "wrapped")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	caller, ok := record["caller"].(string)
	if !ok || !strings.Contains(caller, "config_test.go") {
		t.Fatalf("caller=%v", record["caller"])
	}
}
