//go:build integration

package goose

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"testing/fstest"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	sdk "github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

func TestIntegrationPostgresMigrations(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SERVEX_IT_POSTGRES_DSN not set")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = admin.Close() })
	schema := "servex_goose_" + uuid.NewV7().String()
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config.RuntimeParams["search_path"] = quoted
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	exerciseMigrations(t, db)
}

func exerciseMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	files := fstest.MapFS{
		"00001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE sample (id integer PRIMARY KEY);\n-- +goose Down\nDROP TABLE sample;\n")},
		"00002_insert.sql": {Data: []byte("-- +goose Up\nINSERT INTO sample VALUES (1);\n-- +goose Down\nDELETE FROM sample WHERE id = 1;\n")},
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(sdk.DialectPostgres, db, files, sdk.WithSessionLocker(locker))
	if err != nil {
		t.Fatal(err)
	}
	results, err := m.Up(t.Context())
	if err != nil || len(results) != 2 {
		t.Fatalf("up=%v err=%v", results, err)
	}
	if results, err := m.Up(t.Context()); err != nil || len(results) != 0 {
		t.Fatalf("repeat=%v err=%v", results, err)
	}
	if status, err := m.Status(t.Context()); err != nil || len(status) != 2 {
		t.Fatalf("status=%v err=%v", status, err)
	}
	files["00003_fail.sql"] = &fstest.MapFile{Data: []byte("-- +goose Up\nINSERT INTO sample VALUES (2);\nINSERT INTO missing_table VALUES (1);\n-- +goose Down\nSELECT 1;\n")}
	failing, err := New(sdk.DialectPostgres, db, files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Up(t.Context()); err == nil {
		t.Fatal("failed migration accepted")
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM sample").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rollback count=%d err=%v", count, err)
	}
	if _, err := m.Down(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Down(t.Context()); err != nil {
		t.Fatal(err)
	}
}
