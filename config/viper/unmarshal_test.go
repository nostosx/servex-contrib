package viper_test

import (
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

type TLS struct {
	Enabled bool   `json:"enabled"`
	Cert    string `json:"cert"`
}

type Common struct {
	Name    string `json:"name"`
	Version string
}

type Extra struct {
	Region string `json:"region"`
}

type Server struct {
	Common
	*Extra
	Addr        string            `json:"addr"`
	Port        int               `json:"port"`
	Timeout     time.Duration     `json:"timeout"`
	Idle        time.Duration     `json:"idle"`
	Ratio       float32           `json:"ratio"`
	Retries     uint8             `json:"retries"`
	Debug       bool              `json:"debug"`
	TLS         *TLS              `json:"tls"`
	Tags        []string          `json:"tags"`
	Ports       []int             `json:"ports"`
	Labels      map[string]string `json:"labels"`
	Weights     map[string]int    `json:"weights"`
	ByID        map[int]string    `json:"byId"`
	Backends    []TLS             `json:"backends"`
	IP          netip.Addr        `json:"ip"`
	Raw         any               `json:"raw"`
	Pair        [2]int            `json:"pair"`
	Bytes       []byte            `json:"bytes"`
	MaxConns    int
	Ignored     string `json:"-"`
	ignored     string //nolint:unused // read by reflect.DeepEqual: Unmarshal must leave unexported fields alone
	Opt         *int   `json:"opt"`
	Nullable    *int   `json:"nullable"`
	WithOptions string `json:"with_options,omitempty"`
}

func TestUnmarshal(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"server": map[string]any{
			"name":         "api",
			"VERSION":      "v1",
			"region":       "eu",
			"addr":         ":8000",
			"port":         "8080",
			"timeout":      "5s",
			"idle":         2e9,
			"ratio":        "0.5",
			"retries":      3.0,
			"debug":        "true",
			"tls":          map[string]any{"enabled": true, "cert": "c.pem"},
			"tags":         "single",
			"ports":        []any{"1", 2.0, 3},
			"labels":       map[string]any{"a": "x", "b": 1.0},
			"weights":      map[string]any{"w": "7"},
			"byId":         map[string]any{"10": "ten"},
			"backends":     []any{map[string]any{"cert": "b.pem"}},
			"ip":           "10.0.0.1",
			"raw":          map[string]any{"k": []any{"v"}},
			"pair":         []any{1.0, "2"},
			"bytes":        "raw",
			"maxconns":     "64",
			"Ignored":      "x",
			"ignored":      "x",
			"opt":          5.0,
			"nullable":     nil,
			"with_options": "yes",
			"unknown":      "skipped",
		},
	}))
	out := Server{Extra: &Extra{}, Nullable: new(7), Port: 1}
	if err := c.Unmarshal("server", &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := Server{
		Name:        "api",
		Version:     "v1",
		Extra:       &Extra{Region: "eu"},
		Addr:        ":8000",
		Port:        8080,
		Timeout:     5 * time.Second,
		Idle:        2 * time.Second,
		Ratio:       0.5,
		Retries:     3,
		Debug:       true,
		TLS:         &TLS{Enabled: true, Cert: "c.pem"},
		Tags:        []string{"single"},
		Ports:       []int{1, 2, 3},
		Labels:      map[string]string{"a": "x", "b": "1"},
		Weights:     map[string]int{"w": 7},
		ByID:        map[int]string{10: "ten"},
		Backends:    []TLS{{Cert: "b.pem"}},
		IP:          netip.MustParseAddr("10.0.0.1"),
		Raw:         map[string]any{"k": []any{"v"}},
		Pair:        [2]int{1, 2},
		Bytes:       []byte("raw"),
		MaxConns:    64,
		Opt:         new(5),
		WithOptions: "yes",
		Nullable:    new(7),
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("Unmarshal =\n%+v\nwant\n%+v", out, want)
	}

	// The decoded "any" value must not alias the configuration tree.
	out.Raw.(map[string]any)["k"].([]any)[0] = "mutated"
	if got := c.String("server.raw.k.0"); got != "v" {
		t.Fatalf("server.raw.k.0 = %q after mutating decoded value", got)
	}
}

func TestUnmarshalKeepsDefaults(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"http": map[string]any{"port": 9000.0, "limits": map[string]any{"b": "2"}},
	}))
	out := struct {
		Addr   string         `json:"addr"`
		Port   int            `json:"port"`
		Limits map[string]int `json:"limits"`
	}{Addr: ":80", Port: 80, Limits: map[string]int{"a": 1}}
	if err := c.Unmarshal("http", &out); err != nil {
		t.Fatal(err)
	}
	if out.Addr != ":80" || out.Port != 9000 || !reflect.DeepEqual(out.Limits, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("Unmarshal = %+v", out)
	}
}

func TestUnmarshalMissingPrefix(t *testing.T) {
	c := mustNew(t, src(map[string]any{"a": 1}))
	out := struct{ X int }{X: 3}
	if err := c.Unmarshal("missing.section", &out); err != nil || out.X != 3 {
		t.Fatalf("Unmarshal = %+v, %v", out, err)
	}
}

func TestUnmarshalIntoMapAndRoot(t *testing.T) {
	c := mustNew(t, src(map[string]any{"a": "1", "b": 2.0}))
	var m map[string]any
	if err := c.Unmarshal("", &m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, map[string]any{"a": "1", "b": 2.0}) {
		t.Fatalf("map = %#v", m)
	}
	var ints map[string]int
	if err := c.Unmarshal("", &ints); err != nil || !reflect.DeepEqual(ints, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("ints = %#v, %v", ints, err)
	}
	var n int
	if err := c.Unmarshal("b", &n); err != nil || n != 2 {
		t.Fatalf("scalar = %d, %v", n, err)
	}
}

type Inner struct {
	ID string
}

type Shadow struct {
	ID string
}

type Dominant struct {
	Inner
	Shadow
	ID string
}

type Conflict struct {
	Inner
	Shadow
}

type TaggedID struct {
	ID string `json:"ID"`
}

type TaggedWins struct {
	TaggedID
	B
}

type B struct {
	ID string
}

type unexportedPtr struct{ Field string }

type HasUnexportedPtr struct {
	*unexportedPtr
}

func TestUnmarshalEmbeddingRules(t *testing.T) {
	c := mustNew(t, src(map[string]any{"id": "top", "field": "x"}))

	var d Dominant
	if err := c.Unmarshal("", &d); err != nil {
		t.Fatal(err)
	}
	if d.ID != "top" || d.Inner.ID != "top" || d.Shadow.ID != "top" {
		t.Fatalf("squashed fields must all receive the value: %+v", d)
	}

	var cf Conflict
	if err := c.Unmarshal("", &cf); err != nil {
		t.Fatal(err)
	}
	if cf.Inner.ID != "top" || cf.Shadow.ID != "top" {
		t.Fatalf("Viper populates matching squashed fields: %+v", cf)
	}

	var tw TaggedWins
	if err := c.Unmarshal("", &tw); err != nil {
		t.Fatal(err)
	}
	if tw.TaggedID.ID != "top" || tw.B.ID != "top" {
		t.Fatalf("squashed tagged fields must receive the value: %+v", tw)
	}

	var up HasUnexportedPtr
	err := c.Unmarshal("", &up)
	if err != nil || up.unexportedPtr != nil {
		t.Fatalf("unexported embedded pointer must remain untouched: %+v, %v", up, err)
	}
	// Without a matching key the pointer stays nil and no error occurs.
	var untouched struct {
		*unexportedPtr
		Other string
	}
	c2 := mustNew(t, src(map[string]any{"other": "o"}))
	if err := c2.Unmarshal("", &untouched); err != nil || untouched.unexportedPtr != nil || untouched.Other != "o" {
		t.Fatalf("untouched = %+v, %v", untouched, err)
	}
}

func TestUnmarshalNilEmbeddedPointerStaysNil(t *testing.T) {
	c := mustNew(t, src(map[string]any{"addr": ":1"}))
	var out Server
	if err := c.Unmarshal("", &out); err != nil {
		t.Fatal(err)
	}
	if out.Extra != nil {
		t.Fatalf("Extra allocated without matching keys: %+v", out.Extra)
	}
}

var errBadText = errors.New("bad text")

type badText struct{}

func (*badText) UnmarshalText([]byte) error { return errBadText }

func TestUnmarshalErrors(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"server": map[string]any{
			"port":    "eighty",
			"timeout": "soon",
			"tls":     "yes",
			"ports":   []any{1.0, "x"},
			"labels":  []any{"a"},
			"retries": 300.0,
			"ratio":   1e300,
			"neg":     -1.0,
			"frac":    1.5,
			"ip":      "not-an-ip",
			"pair":    []any{1.0, 2.0, 3.0},
			"bad":     "x",
			"backend": []any{map[string]any{"enabled": "maybe"}},
			"ch":      "x",
			"debug":   2.0,
			"err":     "x",
		},
	}))
	tests := []struct {
		name string
		dst  any
		want string
	}{
		{"int", new(struct {
			Port int `json:"port"`
		}), `config: unmarshal "server.port": cannot convert string "eighty" to int`},
		{"duration", new(struct {
			Timeout time.Duration `json:"timeout"`
		}), `config: unmarshal "server.timeout": cannot convert string "soon" to time.Duration`},
		{"struct from scalar", new(struct {
			TLS *TLS `json:"tls"`
		}), `config: unmarshal "server.tls": cannot convert string "yes" to config_test.TLS`},
		{"slice element", new(struct {
			Ports []int `json:"ports"`
		}), `config: unmarshal "server.ports[1]": cannot convert string "x" to int`},
		{"map from list", new(struct {
			Labels map[string]string `json:"labels"`
		}), `config: unmarshal "server.labels": cannot convert list to map[string]string`},
		{"uint overflow", new(struct {
			Retries uint8 `json:"retries"`
		}), `config: unmarshal "server.retries": value 300 overflows uint8`},
		{"float overflow", new(struct {
			Ratio float32 `json:"ratio"`
		}), `config: unmarshal "server.ratio": value 1e+300 overflows float32`},
		{"negative uint", new(struct {
			Neg uint `json:"neg"`
		}), `config: unmarshal "server.neg": cannot convert float64 -1 to uint`},
		{"fractional int", new(struct {
			Frac int `json:"frac"`
		}), `config: unmarshal "server.frac": cannot convert float64 1.5 to int`},
		{"text unmarshaler", new(struct {
			IP netip.Addr `json:"ip"`
		}), `config: unmarshal "server.ip": ParseAddr("not-an-ip"): unable to parse IP`},
		{"array too long", new(struct {
			Pair [2]int `json:"pair"`
		}), `config: unmarshal "server.pair": list of 3 elements does not fit [2]int`},
		{"nested path", new(struct {
			Backend []TLS `json:"backend"`
		}), `config: unmarshal "server.backend[0].enabled": cannot convert string "maybe" to bool`},
		{"unsupported", new(struct {
			Ch chan int `json:"ch"`
		}), `config: unmarshal "server.ch": unsupported destination type chan int`},
		{"bool from number", new(struct {
			Debug bool `json:"debug"`
		}), `config: unmarshal "server.debug": cannot convert float64 2 to bool`},
		{"custom text error", new(struct {
			Err badText `json:"err"`
		}), `config: unmarshal "server.err": bad text`},
		{"non-empty interface", new(struct {
			Err error `json:"err"`
		}), `config: unmarshal "server.err": cannot convert string "x" to error`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Unmarshal("server", tt.dst)
			switch tt.name {
			case "uint overflow", "float overflow", "negative uint", "fractional int", "bool from number":
				// Native mapstructure weak conversion is intentionally permissive.
				// Applications reject invalid ranges through WithValidator.
				if err != nil {
					t.Fatalf("weak conversion: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("err = %v\nwant  %s", err, tt.want)
			}
		})
	}
}

func TestUnmarshalWrapsTextError(t *testing.T) {
	c := mustNew(t, src(map[string]any{"n": "x"}))
	var dst struct {
		N badText `json:"n"`
	}
	err := c.Unmarshal("", &dst)
	if !errors.Is(err, errBadText) {
		t.Fatalf("err = %v, want wrapping errBadText", err)
	}
}

func TestUnmarshalInvalidDestination(t *testing.T) {
	c := mustNew(t, src(map[string]any{"a": 1}))
	var nilPtr *struct{}
	for _, dst := range []any{nil, struct{}{}, nilPtr} {
		if err := c.Unmarshal("a", dst); err == nil || !strings.HasPrefix(err.Error(), "config: ") {
			t.Errorf("Unmarshal(%T) err = %v", dst, err)
		}
	}
}

func TestUnmarshalNullKeepsDefaults(t *testing.T) {
	c := mustNew(t, src(map[string]any{"s": map[string]any{"p": nil, "n": nil}}))
	out := struct {
		P *int `json:"p"`
		N int  `json:"n"`
	}{P: new(1), N: 5}
	if err := c.Unmarshal("s", &out); err != nil {
		t.Fatal(err)
	}
	if out.P == nil || *out.P != 1 || out.N != 5 {
		t.Fatalf("Viper null must remain unset: %+v", out)
	}
}

func TestUnmarshalNestedPointers(t *testing.T) {
	c := mustNew(t, src(map[string]any{"v": "3"}))
	var out struct {
		V **int `json:"v"`
	}
	if err := c.Unmarshal("", &out); err != nil {
		t.Fatal(err)
	}
	if out.V == nil || *out.V == nil || **out.V != 3 {
		t.Fatalf("V = %v", out.V)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	c, err := viper.New(b.Context(), []config.Source{src(map[string]any{
		"server": map[string]any{"addr": ":8000", "port": "8080", "timeout": "5s", "tags": []any{"a", "b"}},
	})})
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		var out Server
		if err := c.Unmarshal("server", &out); err != nil {
			b.Fatal(err)
		}
	}
}
