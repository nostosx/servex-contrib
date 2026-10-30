package goose

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/DATA-DOG/go-sqlmock"
	sdk "github.com/pressly/goose/v3"
)

func TestConstructionIsExplicitAndIsolated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations := fstest.MapFS{"00001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE example (id integer);\n-- +goose Down\nDROP TABLE example;\n")}}
	m, err := New(sdk.DialectPostgres, db, migrations)
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal("constructor accessed DB", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.Up(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := New(sdk.DialectPostgres, nil, migrations); err == nil {
		t.Fatal("nil DB")
	}
	if _, err := New(sdk.DialectPostgres, db, fstest.MapFS{}); !errors.Is(err, sdk.ErrNoMigrations) {
		t.Fatal(err)
	}
}
