package gorm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	sdkgorm "gorm.io/gorm"

	"github.com/nostosx/servex/app"
)

func TestPostgresResourceSharesPoolAndOwnsLifecycle(t *testing.T) {
	pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultDatabaseConfig()
	cfg.DSN = "host=127.0.0.1 user=fixture"
	options := &sdkgorm.Config{SkipDefaultTransaction: true, Plugins: map[string]sdkgorm.Plugin{}}
	native := options
	db, err := postgresResource("postgres", cfg, pool, nil, nil, native)
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
		cfg := DefaultDatabaseConfig()
		cfg.DSN = "fixture"
		want := errors.New("plugin rejected")
		var native *sdkgorm.Config
		if fail {
			native = &sdkgorm.Config{Plugins: map[string]sdkgorm.Plugin{"failure": failurePlugin{err: want}}}
		}
		mock.ExpectClose()
		db, err := postgresResource("postgres", cfg, pool, nil, nil, native)
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
	cfg := DefaultDatabaseConfig()
	cfg.DSN = "fixture"
	native := &sdkgorm.Config{Plugins: map[string]sdkgorm.Plugin{"panic": panicPlugin{}}}
	mock.ExpectClose()
	func() {
		defer func() {
			if recovered := recover(); recovered != "native plugin panic" {
				t.Fatalf("panic=%v", recovered)
			}
		}()
		_, _ = postgresResource("postgres", cfg, pool, nil, nil, native)
	}()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPostgresValidationAndNoAutomaticConnection(t *testing.T) {
	cfg := DefaultDatabaseConfig()
	cfg.DSN = "host=127.0.0.1 port=1 user=fixture connect_timeout=1"
	db, err := OpenPostgres("postgres", cfg, nil, nil)
	if err != nil {
		t.Fatal("construction should not connect", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*DatabaseConfig){
		func(c *DatabaseConfig) { c.DSN = "" }, func(c *DatabaseConfig) { c.MaxOpenConns = 0 },
		func(c *DatabaseConfig) { c.MaxIdleConns = c.MaxOpenConns + 1 }, func(c *DatabaseConfig) { c.ConnMaxLifetime = -time.Second },
		func(c *DatabaseConfig) { c.DSN = "postgres://%zz" },
	} {
		invalid := cfg
		change(&invalid)
		if db, err := OpenPostgres("postgres", invalid, nil, nil); !errors.Is(err, ErrInvalidDatabaseConfig) || db != nil {
			t.Fatal(db, err)
		}
	}
}

func TestPostgresFailedStartupClosesOwnedPool(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatal(err)
		}
		database, err := postgresResource("database", DefaultDatabaseConfig(), pool, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		pingErr := errors.New("startup ping failed")
		closeErr := errors.New("pool cleanup failed")
		mock.ExpectPing().WillReturnError(pingErr)
		expectClose := mock.ExpectClose()
		if failClose {
			expectClose.WillReturnError(closeErr)
		}
		application := app.New(app.WithHooks(database))
		startErr := application.Start(t.Context())
		if !errors.Is(startErr, pingErr) || (failClose && !errors.Is(startErr, closeErr)) {
			t.Fatal("failed startup lost ping or cleanup identity", startErr)
		}
		if err := database.Check(t.Context()); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatal("failed startup left the resource usable", err)
		}
		if err := database.Close(); (failClose && !errors.Is(err, closeErr)) || (!failClose && err != nil) {
			t.Fatal("cleanup could not be joined", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresCanceledStartupInitiatesCleanup(t *testing.T) {
	pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	database, err := postgresResource("database", DefaultDatabaseConfig(), pool, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	cause := errors.New("startup canceled")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	if err := database.Start(ctx); !errors.Is(err, cause) {
		t.Fatal("startup cancellation identity lost", err)
	}
	// A canceled startup still starts cleanup; Close waits for its completion.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
