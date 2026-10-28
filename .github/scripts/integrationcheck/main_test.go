package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func bashPath(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "windows" {
		return "bash"
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for dir := filepath.Dir(git); ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, "bin", "bash.exe")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		if dir == filepath.Dir(dir) {
			t.Fatal("Git Bash is required for integration gate tests")
		}
	}
}

func quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestIntegrationGate(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../../.."))
	module, config := "cache/redis", []string{"SERVEX_IT_REDIS_ADDR=fixture-redis"}
	valid := []event{{Action: "run", Test: "TestIntegrationRedis"}, {Action: "pass", Test: "TestIntegrationRedis"}}
	goTool := os.Getenv("GO")
	if goTool == "" {
		var err error
		goTool, err = exec.LookPath("go")
		if err != nil {
			t.Fatal(err)
		}
	}
	bash := bashPath(t)
	tests := []struct {
		name         string
		events       []event
		exit         int
		unconfigured bool
		wantOK       bool
	}{
		{name: "complete", events: valid, wantOK: true},
		{name: "missing_configuration", events: valid, unconfigured: true},
		{name: "empty"},
		{name: "skipped", events: append(append([]event{}, valid...), event{Action: "skip", Test: valid[0].Test})},
		{name: "go_failure", events: valid, exit: 7},
		{name: "failed", events: append(append([]event{}, valid...), event{Action: "fail", Test: valid[0].Test})},
		{name: "unfinished", events: append(append([]event{}, valid...), event{Action: "run", Test: valid[0].Test + "/unfinished"})},
		{name: "missing_suite", events: valid[:len(valid)-2]},
	}
	for _, entry := range []string{"script", "task"} {
		for _, tt := range tests {
			t.Run(entry+"/"+tt.name, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "tool with spaces")
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				fake := filepath.Join(dir, "fake-go")
				// Only test execution is synthetic; Go compiles the real validator.
				code := "#!/usr/bin/env bash\nif [ \"$1\" = test ]; then\n  printf '%s\\n' \"$TEST_EVENTS\"\n  exit \"$TEST_EXIT\"\nfi\nexec " + quote(filepath.ToSlash(goTool)) + " \"$@\"\n"
				if err := os.WriteFile(fake, []byte(code), 0o700); err != nil {
					t.Fatal(err)
				}
				var lines []string
				for _, item := range tt.events {
					data, err := json.Marshal(item)
					if err != nil {
						t.Fatal(err)
					}
					lines = append(lines, string(data))
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				args := []string{filepath.ToSlash(filepath.Join(root, ".github/scripts/integration.sh")), module}
				cmd := exec.CommandContext(ctx, bash, args...)
				if entry == "task" {
					cmd = exec.CommandContext(ctx, "task", "--silent", "--dir", root, "test", "INTEGRATION=true", "MODULES="+module)
				}
				cmd.Dir = root
				for _, value := range os.Environ() {
					if !strings.HasPrefix(value, "AGENTX_IT_") && !strings.HasPrefix(value, "SERVEX_IT_") && !strings.HasPrefix(value, "GO=") && !strings.HasPrefix(value, "PATH=") {
						cmd.Env = append(cmd.Env, value)
					}
				}
				path := os.Getenv("PATH")
				if runtime.GOOS == "windows" {
					path = filepath.Dir(bash) + string(os.PathListSeparator) + path
				}
				cmd.Env = append(cmd.Env, "PATH="+path, "GO="+filepath.ToSlash(fake), "GOWORK=off", "TEST_EVENTS="+strings.Join(lines, "\n"), fmt.Sprintf("TEST_EXIT=%d", tt.exit))
				if !tt.unconfigured {
					cmd.Env = append(cmd.Env, config...)
				}
				output, err := cmd.CombinedOutput()
				if (err == nil) != tt.wantOK {
					t.Fatalf("success=%v, want %v: %v\n%s", err == nil, tt.wantOK, err, output)
				}
			})
		}
	}
}
