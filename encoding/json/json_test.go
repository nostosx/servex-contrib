package json_test

import (
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"
)

type user struct {
	ID    int64             `json:"id"`
	Name  string            `json:"name"`
	Tags  []string          `json:"tags"`
	Attrs map[string]string `json:"attrs"`
}

func TestJSONName(t *testing.T) {
	if got := jsoncodec.Codec.Name(); got != "json" {
		t.Fatalf("Name() = %q, want %q", got, "json")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	in := user{ID: 42, Name: "gopher", Tags: []string{"a", "b"}, Attrs: map[string]string{"k": "v"}}
	data, err := jsoncodec.Codec.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out user
	if err := jsoncodec.Codec.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Name != in.Name || !slices.Equal(out.Tags, in.Tags) || !maps.Equal(out.Attrs, in.Attrs) {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

// TestJSONV2Semantics pins behaviors where encoding/json/v2 defaults differ
// from encoding/json v1, proving the codec uses v2 default semantics.
func TestJSONV2Semantics(t *testing.T) {
	t.Run("nil slice and map marshal as empty", func(t *testing.T) {
		data, err := jsoncodec.Codec.Marshal(user{ID: 1})
		if err != nil {
			t.Fatal(err)
		}
		want := `{"id":1,"name":"","tags":[],"attrs":{}}`
		if string(data) != want {
			t.Fatalf("Marshal = %s, want %s", data, want)
		}
	})
	t.Run("field names match case-sensitively", func(t *testing.T) {
		var u user
		if err := jsoncodec.Codec.Unmarshal([]byte(`{"NAME":"x"}`), &u); err != nil {
			t.Fatal(err)
		}
		if u.Name != "" {
			t.Fatalf("case-insensitive match happened: %+v", u)
		}
	})
	t.Run("duplicate names are rejected", func(t *testing.T) {
		var u user
		if err := jsoncodec.Codec.Unmarshal([]byte(`{"id":1,"id":2}`), &u); err == nil {
			t.Fatal("duplicate member name accepted")
		}
	})
	t.Run("invalid UTF-8 is rejected", func(t *testing.T) {
		if _, err := jsoncodec.Codec.Marshal("\xff"); err == nil {
			t.Fatal("invalid UTF-8 accepted")
		}
	})
}

func TestJSONErrors(t *testing.T) {
	tests := []struct {
		name string
		data string
		dst  any
	}{
		{"syntax error", `{"id":`, new(user)},
		{"type mismatch", `{"id":"x"}`, new(user)},
		{"non-pointer destination", `{}`, user{}},
		{"nil destination", `{}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := jsoncodec.Codec.Unmarshal([]byte(tt.data), tt.dst); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	if _, err := jsoncodec.Codec.Marshal(make(chan int)); err == nil {
		t.Fatal("marshaling a channel must fail")
	}
}

func TestJSONConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			in := user{ID: int64(i), Name: strings.Repeat("x", i)}
			data, err := jsoncodec.Codec.Marshal(&in)
			if err != nil {
				t.Error(err)
				return
			}
			var out user
			if err := jsoncodec.Codec.Unmarshal(data, &out); err != nil || out.ID != in.ID || out.Name != in.Name {
				t.Errorf("round trip %d failed: %+v, %v", i, out, err)
			}
		})
	}
	wg.Wait()
}

func BenchmarkJSONMarshal(b *testing.B) {
	in := user{ID: 1, Name: "gopher", Tags: []string{"a"}}
	for b.Loop() {
		if _, err := jsoncodec.Codec.Marshal(&in); err != nil {
			b.Fatal(err)
		}
	}
}
