package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	const workflow = `# BEGIN MODULES
  - alpha
  - discovery/kubernetes
# END MODULES
# BEGIN INTEGRATIONS
  modules: alpha
# END INTEGRATIONS
task test INTEGRATION=true MODULES=discovery/kubernetes
`
	for _, tt := range []struct {
		name string
		old  string
		new  string
	}{
		{name: "complete"},
		{name: "missing_module", old: "  - alpha\n"},
		{name: "duplicate_module", old: "  - alpha\n", new: "  - alpha\n  - alpha\n"},
		{name: "unknown_module", old: "  - alpha\n", new: "  - other\n"},
		{name: "missing_integration", old: "  modules: alpha\n"},
		{name: "duplicate_integration", old: "  modules: alpha\n", new: "  modules: alpha alpha\n"},
		{name: "missing_marker", old: "# END MODULES"},
		{name: "missing_kind", old: "task test INTEGRATION=true MODULES=discovery/kubernetes"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, name := range []string{"alpha/go.mod", "alpha/a_integration_test.go", "discovery/kubernetes/go.mod", "discovery/kubernetes/k_integration_test.go", ".github/workflows/ci.yml"} {
				path := filepath.Join(root, name)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				content := ""
				if name == ".github/workflows/ci.yml" {
					content = workflow
					if tt.old != "" {
						content = strings.ReplaceAll(content, tt.old, tt.new)
					}
				}
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			err := check(root)
			if (err == nil) != (tt.name == "complete") {
				t.Fatalf("check=%v", err)
			}
		})
	}
}
