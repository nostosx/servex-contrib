package gorm

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDatabaseConfigDecodePreservesDefaultsAndExplicitZero(t *testing.T) {
	cfg := DefaultDatabaseConfig()
	if err := json.Unmarshal([]byte(`{"dsn":"host=127.0.0.1 user=fixture","max_idle_conns":0,"conn_max_lifetime":0,"conn_max_idle_time":0}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxOpenConns != 10 || cfg.MaxIdleConns != 0 || cfg.ConnMaxLifetime != 0 || cfg.ConnMaxIdleTime != 0 {
		t.Fatal("decoded values did not preserve omitted defaults and explicit zero")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "GORM") || strings.Contains(string(encoded), "MaxOpenConns") {
		t.Fatal("configuration contains runtime objects or non-file field names")
	}
}

func TestDatabaseConfigValidation(t *testing.T) {
	for _, tt := range []struct {
		field string
		apply func(*DatabaseConfig)
	}{
		{"dsn", func(c *DatabaseConfig) { c.DSN = "" }},
		{"dsn", func(c *DatabaseConfig) { c.DSN = "postgres://fixture:" + "private-value-marker" + "@%zz" }},
		{"max_open_conns", func(c *DatabaseConfig) { c.MaxOpenConns = 0 }},
		{"max_idle_conns", func(c *DatabaseConfig) { c.MaxIdleConns = -1 }},
		{"max_idle_conns", func(c *DatabaseConfig) { c.MaxIdleConns = c.MaxOpenConns + 1 }},
		{"conn_max_lifetime", func(c *DatabaseConfig) { c.ConnMaxLifetime = -time.Second }},
		{"conn_max_idle_time", func(c *DatabaseConfig) { c.ConnMaxIdleTime = -time.Second }},
	} {
		t.Run(tt.field, func(t *testing.T) {
			cfg := DefaultDatabaseConfig()
			cfg.DSN = "host=127.0.0.1 user=fixture"
			tt.apply(&cfg)
			err := cfg.Validate()
			if !errors.Is(err, ErrInvalidDatabaseConfig) || !strings.Contains(err.Error(), tt.field) || strings.Contains(err.Error(), "private-value-marker") {
				t.Fatal("unexpected or unsafe validation result", err)
			}
		})
	}
}
