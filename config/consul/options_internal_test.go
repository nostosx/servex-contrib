package consul

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex/encoding"
)

func TestOptions(t *testing.T) {
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name             string
		opts             []Option
		prefix           bool
		wait, minD, maxD time.Duration
	}{
		{"defaults", []Option{nil, WithCodec(nil)}, false, 0, minRetryDelay, maxRetryDelay},
		{"prefix", []Option{WithPrefix()}, true, 0, minRetryDelay, maxRetryDelay},
		{"wait", []Option{WithWaitTime(time.Minute)}, false, time.Minute, minRetryDelay, maxRetryDelay},
		{"negative wait", []Option{WithWaitTime(-time.Minute)}, false, 0, minRetryDelay, maxRetryDelay},
		{"delays", []Option{WithRetryDelay(time.Second, time.Minute)}, false, 0, time.Second, time.Minute},
		{"non-positive delays", []Option{WithRetryDelay(0, -1)}, false, 0, minRetryDelay, maxRetryDelay},
		{"max below min", []Option{WithRetryDelay(time.Minute, time.Second)}, false, 0, time.Minute, time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(client, "k", tt.opts...)
			if s.prefix != tt.prefix || s.wait != tt.wait || s.minDelay != tt.minD || s.maxDelay != tt.maxD || s.codec != encoding.JSON {
				t.Fatalf("got prefix=%v wait=%v delays=%v/%v codec=%v", s.prefix, s.wait, s.minDelay, s.maxDelay, s.codec)
			}
		})
	}
}
