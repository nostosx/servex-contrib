package database

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const configErrorDriverName = "servex-database-config-error"

type configErrorDriver struct{}

type configDriverError struct{ dsn string }

func (e *configDriverError) Error() string { return "driver rejected " + e.dsn }
func (*configDriverError) Unwrap() error   { return errFake }

func (configErrorDriver) Open(string) (driver.Conn, error) { return nil, errFake }
func (configErrorDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return nil, &configDriverError{dsn: dsn}
}

func TestDatabaseConfigDecodePreservesDefaultsAndExplicitZero(t *testing.T) {
	cfg := DefaultDatabaseConfig()
	if err := json.Unmarshal([]byte(`{"driver":"servex-database-fake-ctx","dsn":"fixture","max_idle_conns":0,"conn_max_lifetime":0,"conn_max_idle_time":0}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxOpenConns != 10 || cfg.MaxIdleConns != 0 || cfg.ConnMaxLifetime != 0 || cfg.ConnMaxIdleTime != 0 {
		t.Fatal("decode did not preserve defaults and explicit zero")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseConfigValidationBeforeOpeningDriver(t *testing.T) {
	for _, tt := range []struct {
		field string
		apply func(*DatabaseConfig)
	}{
		{"driver", func(c *DatabaseConfig) { c.Driver = "" }},
		{"dsn", func(c *DatabaseConfig) { c.DSN = "" }},
		{"max_open_conns", func(c *DatabaseConfig) { c.MaxOpenConns = 0 }},
		{"max_idle_conns", func(c *DatabaseConfig) { c.MaxIdleConns = -1 }},
		{"max_idle_conns", func(c *DatabaseConfig) { c.MaxIdleConns = c.MaxOpenConns + 1 }},
		{"conn_max_lifetime", func(c *DatabaseConfig) { c.ConnMaxLifetime = -time.Second }},
		{"conn_max_idle_time", func(c *DatabaseConfig) { c.ConnMaxIdleTime = -time.Second }},
	} {
		t.Run(tt.field, func(t *testing.T) {
			cfg := DefaultDatabaseConfig()
			cfg.Driver, cfg.DSN = configErrorDriverName, "private-value-marker"
			tt.apply(&cfg)
			db, err := OpenFromConfig(cfg)
			if db != nil || !errors.Is(err, ErrInvalidDatabaseConfig) || !strings.Contains(err.Error(), tt.field) || errors.Is(err, errFake) {
				t.Fatal("invalid settings reached the driver", err)
			}
			if strings.Contains(err.Error(), "private-value-marker") {
				t.Fatal("configuration value disclosed")
			}
		})
	}
}

func TestOpenFromConfigSetsPoolAndInjectsTelemetry(t *testing.T) {
	cfg := DefaultDatabaseConfig()
	cfg.Driver, cfg.DSN = ctxDriverName, "configured-pool"
	cfg.MaxOpenConns, cfg.MaxIdleConns = 3, 0
	cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime = 0, 0
	tracer, meter := &recTracer{}, newRecMeter()
	db, err := OpenFromConfig(cfg, WithTracer(tracer), WithMeter(meter))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if stats := db.Stats(); stats.MaxOpenConnections != 3 || stats.OpenConnections != 0 {
		t.Fatal("constructor connected or ignored pool configuration", stats)
	}
	if _, err := db.ExecContext(t.Context(), "SELECT 42"); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.Idle != 0 || stats.OpenConnections != 0 {
		t.Fatal("zero idle limit ignored", stats)
	}
	if spans := tracer.snapshot(); len(spans) != 1 || spans[0].system != ctxDriverName {
		t.Fatal("configured pool lost telemetry", spans)
	}
	if err := Hook("database", db).Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFromConfigHidesDriverErrorTextAndPreservesIdentity(t *testing.T) {
	cfg := DefaultDatabaseConfig()
	cfg.Driver, cfg.DSN = configErrorDriverName, "private-value-marker"
	db, err := OpenFromConfig(cfg)
	if db != nil || !errors.Is(err, ErrInvalidDatabaseConfig) || !errors.Is(err, errFake) {
		t.Fatal("lost configuration or driver error identity", err)
	}
	if strings.Contains(err.Error(), cfg.DSN) {
		t.Fatal("driver DSN disclosed")
	}
	if native, ok := errors.AsType[*configDriverError](err); !ok || native.dsn != cfg.DSN {
		t.Fatal("lost native driver error type")
	}
	cfg.Driver = "unknown-driver"
	if db, err := OpenFromConfig(cfg); db != nil || !errors.Is(err, ErrInvalidDatabaseConfig) {
		t.Fatal("unknown driver accepted", err)
	}
}

func TestOpenFromConfigAppliesConnectionExpiry(t *testing.T) {
	for _, kind := range []string{"lifetime", "idle"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := DefaultDatabaseConfig()
				cfg.Driver, cfg.DSN = ctxDriverName, "expiry-pool"
				cfg.MaxIdleConns = 1
				cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime = 0, 0
				if kind == "lifetime" {
					cfg.ConnMaxLifetime = time.Second
				} else {
					cfg.ConnMaxIdleTime = time.Second
				}
				db, err := OpenFromConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.ExecContext(t.Context(), "SELECT 42"); err != nil {
					t.Fatal(err)
				}
				if db.Stats().Idle != 1 {
					t.Fatal("fixture connection not retained")
				}
				time.Sleep(3 * time.Second)
				stats := db.Stats()
				if stats.OpenConnections != 0 || (kind == "lifetime" && stats.MaxLifetimeClosed != 1) || (kind == "idle" && stats.MaxIdleTimeClosed != 1) {
					t.Fatal("configured connection expiry not applied", stats)
				}
			})
		})
	}
}
