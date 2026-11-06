package gorm

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sdkgorm "gorm.io/gorm"
)

func TestWithGORMConfigSnapshotsNativeConfiguration(t *testing.T) {
	native := &sdkgorm.Config{SkipDefaultTransaction: true, Plugins: map[string]sdkgorm.Plugin{}}
	options, err := newOptions([]Option{nil, WithGORMConfig(native)})
	if err != nil {
		t.Fatal(err)
	}
	native.SkipDefaultTransaction = false
	native.Plugins["failure"] = failurePlugin{err: errors.New("must not be added")}
	if !options.gorm.SkipDefaultTransaction || len(options.gorm.Plugins) != 0 {
		t.Fatal("runtime options alias caller configuration")
	}
}

func TestOpenPostgresRejectsExternalPoolBeforeConstruction(t *testing.T) {
	pool, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	before := pool.Stats()
	cfg := DefaultDatabaseConfig()
	cfg.DSN = "host=127.0.0.1 port=1 user=fixture"
	if db, err := OpenPostgres("postgres", cfg, nil, nil, WithGORMConfig(&sdkgorm.Config{ConnPool: pool})); db != nil || !errors.Is(err, ErrInvalidDatabaseConfig) {
		t.Fatal("external pool accepted", err)
	}
	if db, err := OpenPostgres("", cfg, nil, nil); db != nil || !errors.Is(err, ErrInvalidDatabaseConfig) {
		t.Fatal("empty resource name accepted", err)
	}
	// Rejection does not close or otherwise acquire the caller's pool.
	if pool.Stats().OpenConnections != before.OpenConnections {
		t.Fatal("validation contacted the database")
	}
}
