// Command integrationcheck validates the JSON results of real-service tests.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type event struct {
	Action  string
	Package string
	Test    string
}

type testKey struct {
	Package string
	Test    string
}

func validate(input io.Reader) (int, error) {
	started, passed := map[testKey]bool{}, map[testKey]bool{}
	failed, skipped := map[testKey]bool{}, map[testKey]bool{}
	reader := bufio.NewReader(input)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		var item event
		// Non-JSON tool diagnostics are covered by the shell's exit-status check.
		if json.Unmarshal(line, &item) == nil && strings.HasPrefix(item.Test, "TestIntegration") {
			key := testKey{item.Package, item.Test}
			switch item.Action {
			case "run":
				started[key] = true
			case "pass":
				passed[key] = true
			case "fail":
				failed[key] = true
			case "skip":
				skipped[key] = true
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	unfinished := 0
	for key := range started {
		if !passed[key] && !failed[key] && !skipped[key] {
			unfinished++
		}
	}
	if len(passed) == 0 || len(failed) != 0 || len(skipped) != 0 || unfinished != 0 {
		return 0, fmt.Errorf("integration results invalid: %d passed, %d failed, %d skipped, %d unfinished", len(passed), len(failed), len(skipped), unfinished)
	}
	return len(passed), nil
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: integrationcheck result-file")
	}
	file, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer file.Close()
	count, err := validate(file)
	if err != nil {
		return err
	}
	fmt.Printf("Integration results: %d tests/subtests passed, none skipped\n", count)
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
