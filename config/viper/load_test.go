package viper_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

var errInvalidPort = errors.New("port is outside supported range")

type loadSettings struct {
	HTTP struct {
		Port    int  `json:"port"`
		Enabled bool `json:"enabled"`
	} `json:"http"`
}

func (c *loadSettings) Validate() error {
	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		return errInvalidPort
	}
	return nil
}

func TestLoadDefaultsLayersAndValidation(t *testing.T) {
	defaults := func() loadSettings {
		var c loadSettings
		c.HTTP.Port, c.HTTP.Enabled = 8000, true
		return c
	}
	t.Setenv("SERVEX_LOAD_HTTP__PORT", "8002")
	sources := []config.Source{
		viper.Values(map[string]any{"http": map[string]any{"port": 8001}}),
		viper.Env("SERVEX_LOAD_"),
		viper.Values(map[string]any{"http.port": "8003", "http.enabled": false}),
	}
	cfg, err := viper.Load(t.Context(), sources, defaults)
	if err != nil || cfg.HTTP.Port != 8003 || cfg.HTTP.Enabled {
		t.Fatalf("merged settings: %+v, %v", cfg, err)
	}
	cfg, err = viper.Load(t.Context(), []config.Source{viper.Values(map[string]any{"http.port": 0})}, defaults)
	if !errors.Is(err, errInvalidPort) || cfg != (loadSettings{}) {
		t.Fatalf("invalid candidate escaped: %+v, %v", cfg, err)
	}
	_, err = viper.Load(t.Context(), []config.Source{viper.Values(map[string]any{"http.port": "invalid"})}, defaults)
	if err == nil {
		t.Fatal("decoding failure accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := viper.Load(ctx, sources, defaults); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestValuesIsolatesInputAndRepeatedLoads(t *testing.T) {
	tree := map[string]any{"http": map[string]any{"port": 8001}, "names": []string{"original"}}
	source := viper.Values(tree)
	tree["http"].(map[string]any)["port"] = 9000
	tree["names"].([]string)[0] = "changed"
	first, err := source.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	first["http"].(map[string]any)["port"] = 9001
	first["names"].([]any)[0] = "changed again"
	second, err := source.Load(t.Context())
	if err != nil || second["http"].(map[string]any)["port"] != 8001 || second["names"].([]any)[0] != "original" {
		t.Fatalf("source mutated: %+v, %v", second, err)
	}
}
