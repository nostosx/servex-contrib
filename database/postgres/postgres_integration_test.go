//go:build integration

package postgres

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestIntegrationExtensionCatalog(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	extensions, err := c.Extensions(t.Context())
	if err != nil || len(extensions) == 0 {
		t.Fatalf("%v: %v", extensions, err)
	}
	names := strings.Fields(os.Getenv("SERVEX_IT_POSTGRES_EXTENSIONS"))
	if len(names) == 0 {
		t.Fatal("explicit SERVEX_IT_POSTGRES_EXTENSIONS required")
	}
	if err := Require(t.Context(), db, names...); err != nil {
		t.Fatal(err)
	}
	// The owned fixture may explicitly enable a bundled extension; constructors never do.
	if err := c.Enable(t.Context(), "pg_trgm"); err != nil {
		t.Fatal(err)
	}
	var similarity float64
	if err := db.QueryRowContext(t.Context(), `SELECT similarity('postgres','postgres')`).Scan(&similarity); err != nil || similarity != 1 {
		t.Fatalf("trigram %f: %v", similarity, err)
	}
	if err := Require(t.Context(), db, "servex_missing_extension"); err == nil {
		t.Fatal("missing extension accepted")
	}
	for _, name := range names {
		switch name {
		case "postgis":
			var distance float64
			if err := db.QueryRowContext(t.Context(), `SELECT ST_Distance(ST_Point(0,0),ST_Point(3,4))`).Scan(&distance); err != nil || distance != 5 {
				t.Fatalf("postgis %f: %v", distance, err)
			}
		case "pg_jsonschema":
			valid, err := c.ValidateJSON(t.Context(), []byte(`{"type":"object","required":["name"]}`), []byte(`{"name":"servex"}`))
			if err != nil || !valid {
				t.Fatalf("schema %v: %v", valid, err)
			}
		case "hypopg":
			var count int
			if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM hypopg_list_indexes`).Scan(&count); err != nil {
				t.Fatal(err)
			}
		case "pg_stat_statements":
			var count int
			if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_stat_statements`).Scan(&count); err != nil {
				t.Fatal(err)
			}
		}
	}
	runExtendedCapabilities(t, db, c)
}
