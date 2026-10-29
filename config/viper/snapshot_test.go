package viper_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

func TestSnapshotPinsRevisionAndCopiesContainers(t *testing.T) {
	t.Parallel()
	original := map[string]any{
		"section": map[string]any{
			"name": "before", "port": "8080", "enabled": "true", "ratio": "1.5", "timeout": "2s",
			"list": []any{map[string]any{"key": "value"}}, "bytes": []byte("abc"),
		},
	}
	source := src(original)
	c := mustNew(t, source)
	before := c.Snapshot()
	copyOfSnapshot := before
	original["section"].(map[string]any)["name"] = "source mutation"
	original["section"].(map[string]any)["bytes"].([]byte)[0] = 'x'

	value, ok := before.Value("SECTION")
	if !ok {
		t.Fatal("missing section")
	}
	section := value.(map[string]any)
	section["name"] = "copy mutation"
	section["list"].([]any)[0].(map[string]any)["key"] = "copy mutation"
	section["bytes"].([]byte)[0] = 'y'
	var decoded map[string]any
	if err := before.Unmarshal("section", &decoded); err != nil {
		t.Fatal(err)
	}
	decoded["name"] = "decode mutation"
	decoded["list"].([]any)[0].(map[string]any)["key"] = "decode mutation"
	decoded["bytes"].([]byte)[0] = 'z'
	var asInterface any
	if err := before.Unmarshal("section.list", &asInterface); err != nil {
		t.Fatal(err)
	}
	asInterface.([]any)[0].(map[string]any)["key"] = "interface mutation"

	source.set(map[string]any{"section": map[string]any{"name": "after", "port": 9090}}, nil)
	if err := c.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []config.Reader{before, copyOfSnapshot} {
		if snapshot.String("section.name") != "before" || snapshot.Int("section.port") != 8080 ||
			!snapshot.Bool("section.enabled") || snapshot.Float64("section.ratio") != 1.5 ||
			snapshot.Duration("section.timeout") != 2*time.Second ||
			snapshot.String("section.list.0.key") != "value" || snapshot.String("section.bytes") != "abc" {
			t.Error("snapshot changed after container mutation or reload")
		}
		var out struct {
			Name string
			Port int
		}
		if err := snapshot.Unmarshal("section", &out); err != nil || out.Name != "before" || out.Port != 8080 {
			t.Errorf("pinned Unmarshal = %+v, %v", out, err)
		}
	}
	if c.String("section.name") != "after" || c.Snapshot().Int("section.port") != 9090 {
		t.Fatal("Config or new snapshot did not advance")
	}
	if _, ok := before.Value("missing"); ok {
		t.Fatal("missing snapshot path was present")
	}
	if err := before.Unmarshal("section", nil); err == nil {
		t.Fatal("snapshot accepted an invalid destination")
	}
}

func TestSnapshotZeroValue(t *testing.T) {
	t.Parallel()
	var snapshot viper.Snapshot
	value, ok := snapshot.Value("")
	if !ok || !reflect.DeepEqual(value, map[string]any{}) {
		t.Fatalf("zero Value = %#v, %v", value, ok)
	}
	value.(map[string]any)["injected"] = true
	if _, ok := snapshot.Value("injected"); ok {
		t.Fatal("zero snapshot was mutated")
	}
	if snapshot.String("missing") != "" || snapshot.Int("missing") != 0 || snapshot.Bool("missing") ||
		snapshot.Float64("missing") != 0 || snapshot.Duration("missing") != 0 {
		t.Fatal("zero snapshot accessors did not return defaults")
	}
	var out any
	if err := snapshot.Unmarshal("", &out); err != nil || !reflect.DeepEqual(out, map[string]any{}) {
		t.Fatalf("zero Unmarshal = %#v, %v", out, err)
	}
	defaults := struct{ Port int }{Port: 8080}
	if err := snapshot.Unmarshal("missing", &defaults); err != nil || defaults.Port != 8080 {
		t.Fatalf("missing Unmarshal = %+v, %v", defaults, err)
	}
}
