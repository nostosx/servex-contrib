package etcd

import (
	"testing"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"
)

func TestOptions(t *testing.T) {
	s := New(nil, "k", nil)
	if s.prefix || s.codec != jsoncodec.Codec {
		t.Fatalf("defaults = prefix %v, codec %v", s.prefix, s.codec)
	}
	if s := New(nil, "k", WithPrefix(), WithCodec(nil)); !s.prefix || s.codec != jsoncodec.Codec {
		t.Fatalf("options = prefix %v, codec %v", s.prefix, s.codec)
	}
}

func TestSegments(t *testing.T) {
	tests := map[string][]string{
		"":       nil,
		"/":      nil,
		"a":      {"a"},
		"/a//b/": {"a", "b"},
		"a/b/c":  {"a", "b", "c"},
	}
	for in, want := range tests {
		got := segments(in)
		if len(got) != len(want) {
			t.Fatalf("segments(%q) = %q, want %q", in, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("segments(%q) = %q, want %q", in, got, want)
			}
		}
	}
}
