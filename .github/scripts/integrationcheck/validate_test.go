package main

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	const passed = `{"Action":"run","Package":"a","Test":"TestIntegrationRedis"}
{"Action":"pass","Package":"a","Test":"TestIntegrationRedis"}
`
	for _, tt := range []struct {
		name  string
		input string
		ok    bool
	}{
		{name: "complete_without_final_newline", input: strings.TrimSpace(passed), ok: true},
		{name: "diagnostics", input: "tool diagnostic\n" + passed, ok: true},
		{name: "long_diagnostic", input: strings.Repeat("x", 128*1024) + "\n" + passed, ok: true},
		{name: "package_isolation", input: passed + `{"Action":"run","Package":"b","Test":"TestIntegrationRedis"}`},
		{name: "unit_only", input: `{"Action":"pass","Test":"TestUnit"}`},
		{name: "package_failure_does_not_count_as_integration", input: `{"Action":"fail","Package":"a"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			count, err := validate(strings.NewReader(tt.input))
			if (err == nil) != tt.ok || (tt.ok && count != 1) {
				t.Fatalf("count=%d error=%v", count, err)
			}
		})
	}
}
