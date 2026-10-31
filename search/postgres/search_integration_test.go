//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	pg "github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/storage/search"
)

func TestIntegrationSearch(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	engines := os.Getenv("SERVEX_IT_SEARCH_ENGINES")
	if engines == "" {
		t.Fatal("SERVEX_IT_SEARCH_ENGINES must explicitly select tested engines")
	}
	for name := range strings.FieldsSeq(engines) {
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s, err := New(db, Config{Namespace: "a", Engine: Engine(name), Table: pg.Table{Name: "sx_search_" + strings.ToLower(rand.Text())}})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cleanup, err := sql.Open("pgx", dsn)
				if err != nil {
					t.Error(err)
					return
				}
				defer cleanup.Close()
				if _, err := cleanup.ExecContext(ctx, "DROP TABLE "+s.table); err != nil {
					t.Error(err)
				}
			})
			if err := s.Upsert(t.Context(), search.Document{ID: "one", Text: "postgres database search", Metadata: map[string]string{"kind": "doc"}, Payload: []byte("safe")}, search.Document{ID: "two", Text: "postgres search", Metadata: map[string]string{"kind": "other"}}); err != nil {
				t.Fatal(err)
			}
			other, err := New(db, Config{Namespace: "b", Engine: Engine(name), Table: s.cfg.Table})
			if err != nil {
				t.Fatal(err)
			}
			if err := other.Upsert(t.Context(), search.Document{ID: "one", Text: "postgres search", Payload: []byte("private")}); err != nil {
				t.Fatal(err)
			}
			if err := s.CreateIndex(t.Context(), "sx_search_idx_"+strings.ToLower(rand.Text())); err != nil {
				t.Fatal(err)
			}
			hits, err := s.Search(t.Context(), search.Query{Text: "postgres", Metadata: map[string]string{"kind": "doc"}, Limit: 10})
			if err != nil || len(hits) != 1 || string(hits[0].Document.Payload) != "safe" {
				t.Fatalf("%v: %v", hits, err)
			}
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.UpsertTx(t.Context(), tx, search.Document{ID: "rollback", Text: "postgres"}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete(t.Context(), "one", "two"); err != nil {
				t.Fatal(err)
			}
			hits, err = other.Search(t.Context(), search.Query{Text: "postgres", Limit: 10})
			if err != nil || len(hits) != 1 {
				t.Fatalf("tenant b: %v: %v", hits, err)
			}
		})
	}
}
