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

func TestFormattingModuleBoundaries(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../../.."))
	taskfile, err := os.ReadFile(filepath.Join(root, "Taskfile.yml"))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "modules with spaces")
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Expose the real internal gate only in the disposable fixture Taskfile.
	write("Taskfile.yml", string(taskfile)+"\n  verify-format:\n    cmds:\n      - task: fmt-check\n")
	write("parent/go.mod", "module parent\n\ngo 1.27.3\n")
	write("parent/nested/go.mod", "module parent/nested\n\ngo 1.27.3\n")
	write("parent/parent.go", "package parent\n")
	const child = "package nested\nfunc broken("
	write("parent/nested/child.go", child)
	gofmt := filepath.Join(runtime.GOROOT(), "bin", "gofmt")
	if runtime.GOOS == "windows" {
		gofmt += ".exe"
	}
	run := func(action, module, formatter string, wantOK bool) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "task", "--silent", "--dir", dir,
			action, "ALL_MODULES=parent parent/nested", "MODULES="+module,
			"GOFMT="+filepath.ToSlash(formatter))
		cmd.Env = append(os.Environ(), "NO_COLOR=1", "CLICOLOR=0", "FORCE_COLOR=0")
		output, err := cmd.CombinedOutput()
		if (err == nil) != wantOK {
			t.Fatalf("%s %s: %v\n%s", action, module, err, output)
		}
		return string(output)
	}
	run("verify-format", "parent", gofmt, true)
	// Build-tagged files belong to the selected module and must still be checked.
	const tagged = "//go:build integration\n\npackage parent\nfunc answer()int{return 42}\n"
	write("parent/source with spaces.go", tagged)
	if output := run("verify-format", "parent", gofmt, false); !strings.Contains(output, "source with spaces.go") {
		t.Fatalf("missing formatting diagnostic: %s", output)
	}
	run("fmt", "parent", gofmt, true)
	run("verify-format", "parent", gofmt, true)
	content, err := os.ReadFile(filepath.Join(dir, "parent/nested/child.go"))
	if err != nil || string(content) != child {
		t.Fatalf("parent formatting changed nested module: %q, %v", content, err)
	}
	write("parent/nested/child.go", "package nested\nfunc answer()int{return 42}\n")
	run("verify-format", "parent/nested", gofmt, false)
	run("fmt", "parent/nested", gofmt, true)
	run("verify-format", "parent/nested", gofmt, true)
	// A formatter can fail without printing a diagnostic; preserve its status.
	failing := filepath.Join(dir, "formatter with spaces")
	if err := os.WriteFile(failing, []byte("#!/usr/bin/env bash\nexit 9\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run("fmt", "parent", failing, false)
	run("verify-format", "parent", failing, false)
}

func TestQualityDiffDiagnostics(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../../.."))
	goTool := os.Getenv("GO")
	if goTool == "" {
		var err error
		goTool, err = exec.LookPath("go")
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		name    string
		command string
		exit    int
	}{
		{name: "tidy_diff", command: "mod", exit: 1},
		{name: "tidy_output_with_zero_exit", command: "mod"},
		{name: "modernizer_diff", command: "fix", exit: 2},
		{name: "modernizer_output_with_zero_exit", command: "fix"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "tool with spaces")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			fake := filepath.Join(dir, "fake-go")
			code := "#!/usr/bin/env bash\n" +
				"if [ \"$1\" = \"$DIFF_COMMAND\" ]; then\n" +
				"  printf '%s\\n' 'quality diff diagnostic'\n" +
				"  exit \"$DIFF_EXIT\"\n" +
				"fi\n" +
				"if [ \"$1\" = fix ]; then exit 0; fi\n" +
				"exec " + quote(filepath.ToSlash(goTool)) + " \"$@\"\n"
			if err := os.WriteFile(fake, []byte(code), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "task", "--silent", "--dir", root,
				"check", "MODULES=observability/logger/slog")
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "GO=") && !strings.HasPrefix(value, "GOFLAGS=") {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "GO="+filepath.ToSlash(fake), "GOFLAGS=",
				"DIFF_COMMAND="+tt.command, "DIFF_EXIT="+fmt.Sprint(tt.exit),
				"NO_COLOR=1", "CLICOLOR=0", "FORCE_COLOR=0")
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("check accepted a dependency or modernizer diff: %s", output)
			}
			if !strings.Contains(string(output), "quality diff diagnostic") {
				t.Fatalf("check hid the diff diagnostic: %v\n%s", err, output)
			}
		})
	}
}

func TestIntegrationGate(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../../.."))
	module, config := "storage/cache/redis", []string{"SERVEX_IT_REDIS_ADDR=fixture-redis"}
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
