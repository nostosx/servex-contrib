//go:build integration

package pgvector

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math"
	random "math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/storage/vector"
)

// Each selected access method owns a separate table. Creating competing indexes
// on one table cannot prove which extension actually served a search.
func TestIntegrationIndexAccessMethods(t *testing.T) {
	kinds := strings.Fields(os.Getenv("SERVEX_IT_VECTOR_INDEX_KINDS"))
	if len(kinds) == 0 {
		return
	}
	seen := map[string]bool{}
	for _, kind := range kinds {
		if seen[kind] || (IndexKind(kind) != DiskANN && IndexKind(kind) != VectorChord && IndexKind(kind) != HNSW && IndexKind(kind) != IVFFlat) {
			t.Fatalf("invalid or repeated index kind %q", kind)
		}
		seen[kind] = true
		for _, metric := range []Metric{Cosine, L2} {
			t.Run(kind+"/"+string(metric), func(t *testing.T) { testIndexAccessMethod(t, IndexKind(kind), metric) })
		}
	}
}

func testIndexAccessMethod(t *testing.T, kind IndexKind, metric Metric) {
	db, err := sql.Open("pgx", os.Getenv("SERVEX_IT_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Keep test planner settings on the same session as the Store's real searches.
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	cfg := Config{Namespace: "tenant_a", Dimensions: 16, Metric: metric, Table: postgres.Table{Name: "sx_ann_" + strings.ToLower(rand.Text())}}
	s, err := New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := db.ExecContext(cleanup, "DROP TABLE "+s.table); err != nil {
			t.Error(err)
		}
	})
	rng := random.New(random.NewPCG(11, 29))
	records := make([]vector.Record, 512)
	for i := range records {
		embedding := make([]float32, 16)
		for j := range embedding {
			embedding[j] = rng.Float32()*2 - 1
		}
		records[i] = vector.Record{ID: fmt.Sprintf("record_%04d", i), Vector: embedding, Metadata: map[string]string{"kind": "doc"}, Payload: []byte("original")}
	}
	if err := s.Upsert(ctx, records...); err != nil {
		t.Fatal(err)
	}
	otherCfg := cfg
	otherCfg.Namespace = "tenant_b"
	other, err := New(db, otherCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Upsert(ctx, vector.Record{ID: "private", Vector: records[0].Vector, Metadata: map[string]string{"kind": "private"}}); err != nil {
		t.Fatal(err)
	}
	index := "sx_ann_index_" + strings.ToLower(rand.Text())
	lists := 0
	if kind == IVFFlat {
		lists = 4
	}
	if err := s.CreateIndex(ctx, index, kind, lists); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE "+s.table+"; SET enable_seqscan=off; SET enable_bitmapscan=off"); err != nil {
		t.Fatal(err)
	}
	embedding, err := s.encode(records[0].Vector)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "EXPLAIN (COSTS OFF) SELECT id FROM "+s.table+" WHERE namespace=$1 AND metadata @> $2::jsonb ORDER BY embedding "+s.operator+" $3::vector LIMIT 1", cfg.Namespace, `{"kind":"doc"}`, embedding)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if !strings.Contains(plan.String(), index) {
		t.Fatalf("selected extension did not serve ANN plan:\n%s", plan.String())
	}
	t.Logf("%s/%s used index %s", kind, metric, index)
	check := func(payload string) {
		t.Helper()
		for _, approx := range []bool{false, true} {
			hits, err := s.Search(ctx, vector.Query{Vector: records[0].Vector, Limit: 1, Metadata: map[string]string{"kind": "doc"}, Approximate: approx})
			if err != nil || len(hits) != 1 || hits[0].Record.ID != records[0].ID || math.Abs(hits[0].Distance) > 1e-5 || string(hits[0].Record.Payload) != payload {
				t.Fatalf("approx=%v hits=%v error=%v", approx, hits, err)
			}
		}
	}
	check("original")
	records[0].Payload = []byte("updated")
	records[0].Vector[0] += 0.25
	if err := s.Upsert(ctx, records[0]); err != nil {
		t.Fatal(err)
	}
	check("updated")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTx(ctx, tx, vector.Record{ID: "rollback", Vector: records[0].Vector}); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+s.table+" WHERE id='rollback'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("rollback count %d: %v", n, err)
	}
	if err := s.Delete(ctx, records[0].ID); err != nil {
		t.Fatal(err)
	}
	hits, err := s.Search(ctx, vector.Query{Vector: records[0].Vector, Limit: 1, Approximate: true})
	if err != nil || len(hits) != 1 || hits[0].Record.ID == records[0].ID || hits[0].Record.ID == "private" {
		t.Fatalf("deleted or foreign record returned: %v %v", hits, err)
	}
	hits, err = other.Search(ctx, vector.Query{Vector: records[0].Vector, Limit: 1, Metadata: map[string]string{"kind": "private"}})
	if err != nil || len(hits) != 1 || hits[0].Record.ID != "private" {
		t.Fatalf("namespace mutation leaked: %v %v", hits, err)
	}
}
