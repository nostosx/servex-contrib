package file

import (
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{"default", nil, defaultPollInterval},
		{"custom", []Option{WithPollInterval(time.Second)}, time.Second},
		{"zero keeps default", []Option{WithPollInterval(0)}, defaultPollInterval},
		{"negative keeps default", []Option{WithPollInterval(-time.Second)}, defaultPollInterval},
		{"nil option ignored", []Option{nil}, defaultPollInterval},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New("x.json", tt.opts...).interval; got != tt.want {
				t.Fatalf("interval = %v, want %v", got, tt.want)
			}
		})
	}
}
