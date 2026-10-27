package yaml_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/encoding/yaml"
	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/config/file"
)

type server struct {
	Addr    string         `yaml:"addr"`
	Port    int            `yaml:"port"`
	Tags    []string       `yaml:"tags"`
	Extra   any            `yaml:"extra"`
	Labels  map[string]any `yaml:"labels"`
	private any
}

func TestName(t *testing.T) {
	if got := yaml.Codec.Name(); got != "yaml" {
		t.Fatalf("Name() = %q, want yaml", got)
	}
}

func TestRoundTrip(t *testing.T) {
	in := server{Addr: ":80", Port: 80, Tags: []string{"a", "b"}, Labels: map[string]any{"k": "v"}}
	data, err := yaml.Codec.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out server
	if err := yaml.Codec.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

func TestUnmarshalStringKeys(t *testing.T) {
	const doc = `
http:
  addr: ":8000"
  port: 8080
  1: one
  true: yes-key
list:
  - name: a
    3.5: x
  - [1, {2: two}]
`
	want := map[string]any{
		"http": map[string]any{"addr": ":8000", "port": 8080, "1": "one", "true": "yes-key"},
		"list": []any{
			map[string]any{"name": "a", "3.5": "x"},
			[]any{1, map[string]any{"2": "two"}},
		},
	}

	t.Run("map", func(t *testing.T) {
		m := map[string]any{}
		if err := yaml.Codec.Unmarshal([]byte(doc), &m); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(m, want) {
			t.Fatalf("got %#v, want %#v", m, want)
		}
	})
	t.Run("any", func(t *testing.T) {
		var v any
		if err := yaml.Codec.Unmarshal([]byte(doc), &v); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(v, any(want)) {
			t.Fatalf("got %#v, want %#v", v, want)
		}
	})
	t.Run("top-level non-string keys", func(t *testing.T) {
		var v any
		if err := yaml.Codec.Unmarshal([]byte("1: a\n2: {3: b}\n"), &v); err != nil {
			t.Fatal(err)
		}
		if w := map[string]any{"1": "a", "2": map[string]any{"3": "b"}}; !reflect.DeepEqual(v, any(w)) {
			t.Fatalf("got %#v, want %#v", v, w)
		}
	})
	t.Run("struct fields", func(t *testing.T) {
		var s struct {
			Extra  any              `yaml:"extra"`
			Labels map[string]any   `yaml:"labels"`
			List   []any            `yaml:"list"`
			Ptr    *any             `yaml:"ptr"`
			Nested []map[string]any `yaml:"nested"`
			Typed  map[string]int   `yaml:"typed"`
		}
		const in = `
extra: {1: a}
labels: {k: {2: b}}
list: [{3: c}]
ptr: {4: d}
nested: [{k: {5: e}}]
typed: {a: 1}
`
		if err := yaml.Codec.Unmarshal([]byte(in), &s); err != nil {
			t.Fatal(err)
		}
		checks := []struct {
			name      string
			got, want any
		}{
			{"extra", s.Extra, map[string]any{"1": "a"}},
			{"labels", s.Labels, map[string]any{"k": map[string]any{"2": "b"}}},
			{"list", s.List, []any{map[string]any{"3": "c"}}},
			{"ptr", *s.Ptr, map[string]any{"4": "d"}},
			{"nested", s.Nested, []map[string]any{{"k": map[string]any{"5": "e"}}}},
			{"typed", s.Typed, map[string]int{"a": 1}},
		}
		for _, c := range checks {
			if !reflect.DeepEqual(c.got, c.want) {
				t.Errorf("%s = %#v, want %#v", c.name, c.got, c.want)
			}
		}
	})
	t.Run("unexported field ignored", func(t *testing.T) {
		var s server
		if err := yaml.Codec.Unmarshal([]byte("extra: {1: a}\n"), &s); err != nil {
			t.Fatal(err)
		}
		if s.private != nil {
			t.Fatal("unexported field written")
		}
		if w := map[string]any{"1": "a"}; !reflect.DeepEqual(s.Extra, any(w)) {
			t.Fatalf("Extra = %#v", s.Extra)
		}
	})
}

func TestUnmarshalEmptyDocument(t *testing.T) {
	m := map[string]any{"keep": true}
	if err := yaml.Codec.Unmarshal(nil, &m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, map[string]any{"keep": true}) {
		t.Fatalf("empty document changed destination: %#v", m)
	}
}

func TestTimestamps(t *testing.T) {
	const doc = "at: 2026-01-02T03:04:05.5Z\nday: 2026-01-02\nquoted: \"2026-01-02\"\n"
	m := map[string]any{}
	if err := yaml.Codec.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"at": "2026-01-02T03:04:05.5Z", "day": "2026-01-02T00:00:00Z", "quoted": "2026-01-02"}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("got %#v, want %#v", m, want)
	}
	var typed struct {
		At time.Time `yaml:"at"`
	}
	if err := yaml.Codec.Unmarshal([]byte(doc), &typed); err != nil {
		t.Fatal(err)
	}
	if w := time.Date(2026, 1, 2, 3, 4, 5, 5e8, time.UTC); !typed.At.Equal(w) {
		t.Fatalf("typed At = %v, want %v", typed.At, w)
	}

	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, doc)
	c, err := config.New(t.Context(), []config.Source{file.New(path, file.WithCodec(".yaml", yaml.Codec))})
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		At time.Time `json:"at"`
	}
	if err := c.Unmarshal("", &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.At.Equal(typed.At) {
		t.Fatalf("config At = %v, want %v", cfg.At, typed.At)
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name string
		data string
		dst  any
	}{
		{"syntax error", "a: [", new(map[string]any)},
		{"type mismatch", "port: abc", new(server)},
		{"non-pointer destination", "a: 1", server{}},
		{"nil destination", "a: 1", nil},
		{"nil pointer", "a: 1", (*server)(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := yaml.Codec.Unmarshal([]byte(tt.data), tt.dst)
			if err == nil || !strings.HasPrefix(err.Error(), "yaml: ") {
				t.Fatalf("err = %v, want a yaml error", err)
			}
		})
	}
	if _, err := yaml.Codec.Marshal(make(chan int)); err == nil || !strings.HasPrefix(err.Error(), "yaml: ") {
		t.Fatalf("Marshal(chan) err = %v, want a yaml error", err)
	}
	if _, err := yaml.Codec.Marshal(func() {}); err == nil {
		t.Fatal("Marshal(func) must fail")
	}
	var nilPtr *server
	err := yaml.Codec.Unmarshal([]byte("a: 1"), nilPtr)
	if err == nil || errors.Unwrap(err) == nil {
		t.Fatalf("err = %v, want wrapped sentinel", err)
	}
}

func TestConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			in := map[string]any{"i": i, "nested": map[string]any{"s": strings.Repeat("x", i)}}
			data, err := yaml.Codec.Marshal(in)
			if err != nil {
				t.Error(err)
				return
			}
			out := map[string]any{}
			if err := yaml.Codec.Unmarshal(data, &out); err != nil || !reflect.DeepEqual(out, in) {
				t.Errorf("round trip %d = %#v, %v", i, out, err)
			}
		})
	}
	wg.Wait()
}

func TestConfigWithFileSource(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.yaml")
	over := filepath.Join(dir, "override.yml")
	writeFile(t, base, `
http:
  addr: ":8000"
  timeout: 1s
  headers:
    1: one
broker:
  kafka:
    brokers: [a, b]
`)
	writeFile(t, over, `
http:
  timeout: 2s
  headers:
    2: two
`)
	c, err := config.New(t.Context(), []config.Source{
		file.New(base, file.WithCodec(".yaml", yaml.Codec)),
		file.New(over, file.WithCodec(".yml", yaml.Codec)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.String("http.addr"); got != ":8000" {
		t.Errorf("http.addr = %q", got)
	}
	if got := c.Duration("http.timeout"); got != 2*time.Second {
		t.Errorf("http.timeout = %v", got)
	}
	if got := c.String("http.headers.1"); got != "one" {
		t.Errorf("http.headers.1 = %q", got)
	}
	if got := c.String("http.headers.2"); got != "two" {
		t.Errorf("http.headers.2 = %q", got)
	}
	var cfg struct {
		Kafka struct {
			Brokers []string `json:"brokers"`
		} `json:"kafka"`
	}
	if err := c.Unmarshal("broker", &cfg); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(cfg.Kafka.Brokers, want) {
		t.Errorf("brokers = %v, want %v", cfg.Kafka.Brokers, want)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
