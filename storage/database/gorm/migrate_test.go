package gorm

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAutoMigrateLockFailureRollsBackAndRetainsPool(t *testing.T) {
	pool, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("lock refused")
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnError(want)
	mock.ExpectRollback()
	if err := AutoMigrate(t.Context(), pool, &struct{ ID int }{}); !errors.Is(err, want) {
		t.Fatal(err)
	}
	// The borrowed pool is still usable after a migration failure.
	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := pool.ExecContext(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAutoMigrateRejectsCanceledContextAndClosedOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := AutoMigrate(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := AutoMigrate(t.Context(), nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	db := &Database{}
	db.closed.Store(true)
	if err := db.AutoMigrate(t.Context()); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatal(err)
	}
}
