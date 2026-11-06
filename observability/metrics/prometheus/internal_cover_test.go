package prometheus

import (
	"errors"
	"strings"
	"testing"
)

func TestKindString(t *testing.T) {
	tests := []struct {
		k    kind
		want string
	}{
		{kindCounter, "counter"},
		{kindGauge, "gauge"},
		{kindHistogram, "histogram"},
		{0, "kind(0)"},
		{42, "kind(42)"},
	}
	for _, tt := range tests {
		if got := tt.k.String(); got != tt.want {
			t.Errorf("kind(%d).String() = %q, want %q", uint8(tt.k), got, tt.want)
		}
	}
}

func TestResolve(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name    string
		get     func(...string) (int, error)
		want    int
		wantErr string
	}{
		{"value", func(v ...string) (int, error) { return len(v), nil }, 2, ""},
		{"error", func(...string) (int, error) { return 0, boom }, 0, "boom"},
		{"panic", func(...string) (int, error) { panic("inconsistent cardinality") }, 0,
			"metrics/prometheus: inconsistent cardinality"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolve(tt.get, []string{"a", "b"})
			if got != tt.want {
				t.Errorf("resolve() = %d, want %d", got, tt.want)
			}
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("resolve() error = %v, want nil", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("resolve() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
