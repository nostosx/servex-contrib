//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Capabilities are selected explicitly by the fixture profile. Unknown selections
// fail instead of silently skipping a server whose extension was not installed.
func runExtendedCapabilities(t *testing.T, db *sql.DB, c *Client) {
	t.Helper()
	tests := map[string]func(*testing.T, *sql.DB, *Client){
		"jsonschema": testJSONSchema, "ivm": testIncrementalView, "age": testGraph,
		"partman": testPartitions, "spatial": testSpatial, "hypopg": testHypotheticalIndex,
		"statistics": testStatementStatistics, "audit": testAudit, "repack": testRepack,
		"net": testAsyncHTTP, "fdw": testForeignDatabase, "timescale": testTimescale,
		"citus": testDistributedTables, "duckdb": testDuckDB,
	}
	seen := map[string]bool{}
	for name := range strings.FieldsSeq(os.Getenv("SERVEX_IT_POSTGRES_CAPABILITIES")) {
		test, ok := tests[name]
		if !ok || seen[name] {
			t.Fatalf("unknown or repeated capability %q", name)
		}
		seen[name] = true
		t.Run(name, func(t *testing.T) { test(t, db, c) })
	}
}

type capability struct {
	ctx            context.Context
	conn           *sql.Conn
	db             *sql.DB
	schema, quoted string
}

func ownCapability(t *testing.T, db *sql.DB) *capability {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	name := "sx_ext_" + strings.ToLower(rand.Text())
	quoted, err := Ident(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		cancel()
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &capability{ctx: ctx, conn: conn, db: db, schema: name, quoted: quoted}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		_, _ = conn.ExecContext(cleanup, "RESET ALL")
		_ = conn.Close()
		if _, err := db.ExecContext(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		cancel()
	})
	return f
}
func (f *capability) table(name string) string { return f.quoted + `."` + name + `"` }
func (f *capability) exec(t *testing.T, statement string, args ...any) {
	t.Helper()
	if _, err := f.conn.ExecContext(f.ctx, statement, args...); err != nil {
		t.Fatal(err)
	}
}
func (f *capability) integer(t *testing.T, statement string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.conn.QueryRowContext(f.ctx, statement, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func (f *capability) text(t *testing.T, statement string, args ...any) string {
	t.Helper()
	var s string
	if err := f.conn.QueryRowContext(f.ctx, statement, args...).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func (f *capability) plan(t *testing.T, statement string) string {
	t.Helper()
	rows, err := f.conn.QueryContext(f.ctx, "EXPLAIN (VERBOSE,COSTS OFF) "+statement)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}
func requireCapability(t *testing.T, db *sql.DB, names ...string) {
	t.Helper()
	if err := Require(t.Context(), db, names...); err != nil {
		t.Fatal(err)
	}
}
func waitCapability(t *testing.T, ctx context.Context, check func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Errorf("capability did not complete: %w", ctx.Err()))
		case <-ticker.C:
		}
	}
}
