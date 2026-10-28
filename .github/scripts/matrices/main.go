// Command matrices checks that CI covers every module and integration directory.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

func section(workflow, name string) (string, error) {
	_, rest, ok := strings.Cut(workflow, "# BEGIN "+name)
	if !ok {
		return "", fmt.Errorf("missing BEGIN %s marker", name)
	}
	content, _, ok := strings.Cut(rest, "# END "+name)
	if !ok {
		return "", fmt.Errorf("missing END %s marker", name)
	}
	return content, nil
}

func compare(label string, expected map[string]bool, actual []string) error {
	seen := map[string]bool{}
	for _, name := range actual {
		if seen[name] || !expected[name] {
			return fmt.Errorf("%s contains duplicate or unknown directory: %s", label, name)
		}
		seen[name] = true
	}
	var missing []string
	for name := range expected {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		slices.Sort(missing)
		return fmt.Errorf("%s misses directories: %v", label, missing)
	}
	fmt.Printf("%s: %d directories covered\n", label, len(seen))
	return nil
}

func check(root string) error {
	modules, integrations := map[string]bool{}, map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Name() != "go.mod" && !strings.HasSuffix(entry.Name(), "_integration_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		if entry.Name() == "go.mod" {
			modules[filepath.ToSlash(rel)] = true
		} else {
			integrations[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(root, ".github/workflows/ci.yml"))
	if err != nil {
		return err
	}
	workflow := string(data)
	moduleSection, err := section(workflow, "MODULES")
	if err != nil {
		return err
	}
	var listed []string
	for _, match := range regexp.MustCompile(`(?m)^\s+- ([a-z0-9/]+)\s*$`).FindAllStringSubmatch(moduleSection, -1) {
		listed = append(listed, match[1])
	}
	if err := compare("Module matrix", modules, listed); err != nil {
		return err
	}
	integrationSection, err := section(workflow, "INTEGRATIONS")
	if err != nil {
		return err
	}
	listed = nil
	for _, match := range regexp.MustCompile(`(?m)^\s+modules: (.+)$`).FindAllStringSubmatch(integrationSection, -1) {
		listed = append(listed, strings.Fields(match[1])...)
	}
	if !strings.Contains(workflow, "task test INTEGRATION=true MODULES=discovery/kubernetes") {
		return errors.New("missing kind integration invocation")
	}
	listed = append(listed, "discovery/kubernetes")
	return compare("Integration matrix", integrations, listed)
}

func main() {
	if err := check("."); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
