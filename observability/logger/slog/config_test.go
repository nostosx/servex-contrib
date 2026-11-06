package slog

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"io"
	standardslog "log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nostosx/servex/observability/logger"
)

type borrowedWriter struct {
	bytes.Buffer
	closes int
}

func (w *borrowedWriter) Close() error { w.closes++; return nil }

func TestConfigDecodedFieldsAndActualOutput(t *testing.T) {
	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"level":"warn","format":"json","output":"stderr","add_source":true}`), &cfg); err != nil {
		t.Fatal(err)
	}
	var writer borrowedWriter
	log, err := New(cfg, WithWriter(&writer))
	if err != nil {
		t.Fatal(err)
	}
	if log.Enabled(t.Context(), logger.LevelInfo) || !log.Enabled(t.Context(), logger.LevelWarn) {
		t.Fatal("configured level not applied")
	}
	log.Log(t.Context(), logger.LevelInfo, "hidden")
	log.Log(t.Context(), logger.LevelWarn, "visible", "order_id", 42)
	var record map[string]any
	if err := json.Unmarshal(writer.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	source, ok := record["source"].(map[string]any)
	if !ok || !strings.Contains(source["file"].(string), "config_test.go") {
		t.Fatalf("source=%v", record["source"])
	}
	if record["msg"] != "visible" || record["level"] != "WARN" || record["order_id"] != float64(42) {
		t.Fatalf("record=%v", record)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if writer.closes != 0 {
		t.Fatal("borrowed writer closed")
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
			if !strings.Contains(output.String(), "level=INFO") || !strings.Contains(output.String(), "msg=hello") {
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
	for _, cfg := range []Config{{Level: "invalid", Output: path}, {Format: "invalid", Output: path}, {Output: "bad\x00output"}} {
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
	for _, writer := range []io.Writer{nil, (*borrowedWriter)(nil)} {
		if log, err := New(Config{Output: path}, WithWriter(writer)); !errors.Is(err, ErrInvalidConfig) || log != nil {
			t.Fatalf("log=%v err=%v", log, err)
		}
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

func TestStandardOutputsAreBorrowed(t *testing.T) {
	for _, output := range []string{"stdout", "stderr"} {
		log, err := New(Config{Output: output})
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
	if err := Wrap(nil).Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWrappedSourceLocation(t *testing.T) {
	var output bytes.Buffer
	native := standardslog.New(standardslog.NewJSONHandler(&output, &standardslog.HandlerOptions{AddSource: true}))
	log := Wrap(native)
	log.Log(t.Context(), logger.LevelInfo, "wrapped")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	source, ok := record["source"].(map[string]any)
	if !ok || !strings.Contains(source["file"].(string), "config_test.go") {
		t.Fatalf("source=%v", record["source"])
	}
}
