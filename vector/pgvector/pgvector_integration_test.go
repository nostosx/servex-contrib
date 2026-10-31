//go:build integration

package pgvector

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/storage/vector"
)

func integrationStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db, Config{Namespace: "tenant_a", Dimensions: 3, Table: postgres.Table{Name: "sx_vector_" + strings.ToLower(rand.Text())}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "DROP TABLE "+s.table); err != nil {
			t.Error(err)
		}
	})
	return s, db
}
func TestIntegrationStore(t *testing.T) {
	s, db := integrationStore(t)
	if err := s.Upsert(t.Context(), vector.Record{ID: "one", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"kind": "doc"}, Payload: []byte("first")}, vector.Record{ID: "two", Vector: []float32{0, 1, 0}, Metadata: map[string]string{"kind": "other"}}); err != nil {
		t.Fatal(err)
	}
	other, err := New(db, Config{Namespace: "tenant_b", Dimensions: 3, Table: s.cfg.Table})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Upsert(t.Context(), vector.Record{ID: "one", Vector: []float32{1, 0, 0}, Payload: []byte("private")}); err != nil {
		t.Fatal(err)
	}
	for _, approx := range []bool{false, true} {
		hits, err := s.Search(t.Context(), vector.Query{Vector: []float32{1, 0, 0}, Limit: 10, Metadata: map[string]string{"kind": "doc"}, Approximate: approx})
		if err != nil || len(hits) != 1 || string(hits[0].Record.Payload) != "first" || hits[0].Distance != 0 {
			t.Fatalf("%v: %v", hits, err)
		}
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTx(t.Context(), tx, vector.Record{ID: "rollback", Vector: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+s.table+" WHERE id='rollback'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback %d: %v", count, err)
	}
	if err := s.CreateIndex(t.Context(), "sx_hnsw_"+strings.ToLower(rand.Text()), HNSW, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateIndex(t.Context(), "sx_ivf_"+strings.ToLower(rand.Text()), IVFFlat, 1); err != nil {
		t.Fatal(err)
	}
	for name := range strings.FieldsSeq(os.Getenv("SERVEX_IT_VECTOR_INDEX_KINDS")) {
		if err := s.CreateIndex(t.Context(), "sx_extra_"+strings.ToLower(rand.Text()), IndexKind(name), 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, approx := range []bool{false, true} {
		hits, err := s.Search(t.Context(), vector.Query{Vector: []float32{1, 0, 0}, Limit: 10, Approximate: approx})
		if err != nil || len(hits) != 2 || hits[0].Record.ID != "one" {
			t.Fatalf("indexed: %v: %v", hits, err)
		}
	}
	if err := s.Delete(t.Context(), "one", "two"); err != nil {
		t.Fatal(err)
	}
	hits, err := other.Search(t.Context(), vector.Query{Vector: []float32{1, 0, 0}, Limit: 10})
	if err != nil || len(hits) != 1 {
		t.Fatalf("other tenant affected: %v: %v", hits, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Search(cancelled, vector.Query{Vector: []float32{1, 0, 0}, Limit: 1}); err == nil {
		t.Fatal("ignored cancellation")
	}
}
