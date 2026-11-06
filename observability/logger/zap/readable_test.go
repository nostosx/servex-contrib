package zap

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex/observability/logger"
)

func TestNewConsoleAndFileFormats(t *testing.T) {
	var console bytes.Buffer
	path := filepath.Join(t.TempDir(), "nested", "app.log")
	log, close, err := newReadable(testConfig("warn"), &console, path)
	if err != nil {
		t.Fatal(err)
	}
	if log.Enabled(t.Context(), logger.LevelInfo) {
		t.Fatal("info enabled at warn threshold")
	}
	log.Log(t.Context(), logger.LevelInfo, "hidden")
	_, source, line, _ := runtime.Caller(0)
	log.Log(t.Context(), logger.LevelWarn, "visible", "operation", "get_order", "attempt", 2, "duration", 12*time.Millisecond)
	if err := close(); err != nil {
		t.Fatal(err)
	}
	text := console.String()
	caller := filepath.Base(source) + ":" + strconv.Itoa(line+1)
	if strings.HasPrefix(text, "{") || !strings.Contains(text, "WARN") || !strings.Contains(text, caller) || !strings.Contains(text, "[耗时:12ms]") || !strings.Contains(text, "[Zap] visible") || !strings.Contains(text, "[操作:get_order]") || strings.Contains(text, "hidden") {
		t.Fatal(text)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "WARN" || record["msg"] != "visible" || record["operation"] != "get_order" || record["attempt"] != float64(2) || record["duration"] != 0.012 {
		t.Fatal(record)
	}
	timestamp, ok := record["timestamp"].(string)
	if !ok {
		t.Fatal(record)
	}
	if len(timestamp) != 19 {
		t.Fatal("timestamp must use second precision", timestamp)
	}
	if _, err := time.Parse("2006-01-02 15:04:05", timestamp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(text, timestamp+"\t") {
		t.Fatal("console and file timestamps differ", text, timestamp)
	}
	if call, ok := record["caller"].(string); !ok || !strings.HasSuffix(call, caller) {
		t.Fatal(record)
	}
	if _, oldKey := record["ts"]; oldKey {
		t.Fatal(record)
	}
}

type failingSyncWriter struct {
	bytes.Buffer
	err error
}

func (w *failingSyncWriter) Sync() error { return w.err }

func TestNewPreservesFlushFailure(t *testing.T) {
	sentinel := errors.New("flush failed")
	_, close, err := newReadable(testConfig(""), &failingSyncWriter{err: sentinel}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := close(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if log, close, err := newReadable(testConfig("invalid"), nil, ""); err == nil || log != nil || close != nil {
		t.Fatal("invalid level accepted")
	}
}

func TestDefaultLoggerClose(t *testing.T) {
	_, close, err := newReadable(testConfig(""), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsInvalidFileDestination(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if log, close, err := newReadable(testConfig(""), &bytes.Buffer{}, filepath.Join(parent, "app.log")); err == nil || log != nil || close != nil {
		t.Fatal("invalid file destination accepted")
	}
}

func TestNewAppendsExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("existing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, close, err := newReadable(testConfig(""), &bytes.Buffer{}, path)
	if err != nil {
		t.Fatal(err)
	}
	log.Log(t.Context(), logger.LevelInfo, "appended")
	if err := close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(content, []byte("existing\n")) || !bytes.Contains(content, []byte("appended")) {
		t.Fatal(string(content))
	}
}

func TestConsoleEscapesLinesAndPreservesFileFields(t *testing.T) {
	var console bytes.Buffer
	path := filepath.Join(t.TempDir(), "app.log")
	log, close, err := newReadable(testConfig(""), &console, path)
	if err != nil {
		t.Fatal(err)
	}
	log.Log(t.Context(), logger.LevelError, "failed\nnext", "component", "rpc", "error", errors.New("private\nreason"), "retry", true, "items", []int{1, 2})
	if err := close(); err != nil {
		t.Fatal(err)
	}
	text := console.String()
	if strings.Count(text, "\n") != 1 || !strings.Contains(text, "[RPC] failed\\nnext") || !strings.Contains(text, "[错误:private\\nreason]") || !strings.Contains(text, "[retry:true]") {
		t.Fatal(text)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "failed\nnext" || record["error"] != "private\nreason" || record["retry"] != true {
		t.Fatal(record)
	}
	if items, ok := record["items"].([]any); !ok || len(items) != 2 {
		t.Fatal(record)
	}
}

func testConfig(level string) Config {
	cfg := DefaultConfig()
	cfg.Format, cfg.AddSource = "readable", true
	cfg.Labels = map[string]string{"operation": "操作", "duration": "耗时", "error": "错误"}
	cfg.Level = level
	return cfg
}

func TestNewConfiguredJSONOutput(t *testing.T) {
	cfg := testConfig("info")
	cfg.Format = "json"
	cfg.AddSource = false
	cfg.Output = filepath.Join(t.TempDir(), "output.log")
	log, close, err := newReadable(cfg, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	log.Log(t.Context(), logger.LevelInfo, "configured", "operation", "account")
	if err := close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.Output)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "configured" || record["operation"] != "account" {
		t.Fatal(record)
	}
	if _, ok := record["caller"]; ok {
		t.Fatal("source locations enabled despite add_source=false", record)
	}
}

func newReadable(cfg Config, writer io.Writer, path string) (*Logger, func() error, error) {
	cfg.File.Path = path
	options := []Option{}
	if writer != nil {
		options = append(options, WithWriter(writer))
	}
	log, err := New(cfg, options...)
	if err != nil {
		return nil, nil, err
	}
	return log, log.Close, nil
}

func TestConfiguredFileActuallyRotates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.File = FileConfig{Path: filepath.Join(t.TempDir(), "app.log"), MaxSize: 1, MaxBackups: 2}
	log, err := New(cfg, WithWriter(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		log.Log(t.Context(), logger.LevelInfo, "rotation", "payload", strings.Repeat("x", 256<<10))
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	backups, err := filepath.Glob(filepath.Join(filepath.Dir(cfg.File.Path), "app-*.log"))
	if err != nil || len(backups) == 0 {
		t.Fatal("configured maximum size did not rotate the file", backups, err)
	}
}

func TestBorrowedWriterReplacesConfiguredPrimaryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	cfg := Config{Output: path, File: FileConfig{Path: path}}
	var console bytes.Buffer
	log, err := New(cfg, WithWriter(&console))
	if err != nil {
		t.Fatal("ignored primary output conflicted with the rotating file", err)
	}
	log.Log(t.Context(), logger.LevelInfo, "separate outputs")
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Count(string(data), "\n") != 1 || strings.Count(console.String(), "\n") != 1 {
		t.Fatal("outputs did not receive one record each", err)
	}
	if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("duplicate owned file outputs accepted", err)
	}
}
