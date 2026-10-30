package gorm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	sdkgorm "gorm.io/gorm"
)

func TestPostgresResourceSharesPoolAndOwnsLifecycle(t *testing.T) {
	pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultPostgresConfig("host=127.0.0.1 user=fixture")
	options := &sdkgorm.Config{SkipDefaultTransaction: true, Plugins: map[string]sdkgorm.Plugin{}}
	cfg.GORM = options
	db, err := postgresResource("postgres", cfg, pool, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal("constructor contacted DB", err)
	}
	actual, err := db.GORM().DB()
	if err != nil || actual != db.SQL() || db.SQL() != pool {
		t.Fatal("multiple pools", err)
	}
	if db.SQL().Stats().MaxOpenConnections != cfg.MaxOpenConns {
		t.Fatal(db.SQL().Stats())
	}
	if options.DisableAutomaticPing || options.Logger != nil || len(options.Plugins) != 0 {
		t.Fatal("mutated native config")
	}
	for range 2 {
		mock.ExpectPing()
		if err := db.Check(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	mock.ExpectQuery("SELECT 42").WillReturnRows(sqlmock.NewRows([]string{"answer"}).AddRow(42))
	var answer int
	if err := db.GORM().WithContext(t.Context()).Raw("SELECT 42").Scan(&answer).Error; err != nil || answer != 42 {
		t.Fatal(answer, err)
	}
	mock.ExpectClose()
	if err := db.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Start(t.Context()); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresResourceCloseBeforeStartAndFailureCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatal(err)
		}
		cfg := DefaultPostgresConfig("fixture")
		want := errors.New("plugin rejected")
		if fail {
			cfg.GORM = &sdkgorm.Config{Plugins: map[string]sdkgorm.Plugin{"failure": failurePlugin{err: want}}}
		}
		mock.ExpectClose()
		db, err := postgresResource("postgres", cfg, pool, nil, nil)
		if fail {
			if !errors.Is(err, want) || db != nil {
				t.Fatal("lost constructor error", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			if err := db.Stop(canceled); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

type failurePlugin struct{ err error }

func (failurePlugin) Name() string                   { return "failure" }
func (p failurePlugin) Initialize(*sdkgorm.DB) error { return p.err }

type panicPlugin struct{}

func (panicPlugin) Name() string                 { return "panic" }
func (panicPlugin) Initialize(*sdkgorm.DB) error { panic("native plugin panic") }

func TestNativePluginPanicStillClosesPool(t *testing.T) {
	pool, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultPostgresConfig("fixture")
	cfg.GORM = &sdkgorm.Config{Plugins: map[string]sdkgorm.Plugin{"panic": panicPlugin{}}}
	mock.ExpectClose()
	func() {
		defer func() {
			if recovered := recover(); recovered != "native plugin panic" {
				t.Fatalf("panic=%v", recovered)
			}
		}()
		_, _ = postgresResource("postgres", cfg, pool, nil, nil)
	}()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPostgresValidationAndNoAutomaticConnection(t *testing.T) {
	cfg := DefaultPostgresConfig("host=127.0.0.1 port=1 user=fixture connect_timeout=1")
	db, err := OpenPostgres("postgres", cfg, nil, nil)
	if err != nil {
		t.Fatal("construction should not connect", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*PostgresConfig){
		func(c *PostgresConfig) { c.DSN = "" }, func(c *PostgresConfig) { c.MaxOpenConns = 0 },
		func(c *PostgresConfig) { c.MaxIdleConns = c.MaxOpenConns + 1 }, func(c *PostgresConfig) { c.ConnMaxLifetime = -time.Second },
		func(c *PostgresConfig) { c.DSN = "postgres://%zz" },
	} {
		invalid := cfg
		change(&invalid)
		if db, err := OpenPostgres("postgres", invalid, nil, nil); !errors.Is(err, ErrInvalidPostgresConfig) || db != nil {
			t.Fatal(db, err)
		}
	}
}
