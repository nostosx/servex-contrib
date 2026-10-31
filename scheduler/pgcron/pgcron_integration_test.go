//go:build integration

package pgcron

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestIntegrationSchedules(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(db, "sx_cron_"+strings.ToLower(rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A far-future schedule validates persistence without executing arbitrary work.
	id, err := s.Schedule(t.Context(), "probe", "0 0 1 1 *", "SELECT 1")
	if err != nil {
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
		other, _ := New(cleanup, strings.TrimSuffix(s.prefix, ":"))
		if _, err := other.Unschedule(ctx, "probe"); err != nil {
			t.Error(err)
		}
	})
	id2, err := s.Schedule(t.Context(), "probe", "0 0 2 1 *", "SELECT 2")
	if err != nil || id != id2 {
		t.Fatalf("update: %d %d: %v", id, id2, err)
	}
	jobs, err := s.Jobs(t.Context())
	if err != nil || len(jobs) != 1 || jobs[0].Command != "SELECT 2" {
		t.Fatalf("%v: %v", jobs, err)
	}
	other, err := New(db, "sx_other_"+strings.ToLower(rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err = other.Jobs(t.Context())
	if err != nil || len(jobs) != 0 {
		t.Fatalf("scope: %v: %v", jobs, err)
	}
	if removed, err := s.Unschedule(t.Context(), "probe"); err != nil || !removed {
		t.Fatalf("remove: %v %v", removed, err)
	}
	if removed, err := s.Unschedule(t.Context(), "probe"); err != nil || removed {
		t.Fatalf("idempotency: %v %v", removed, err)
	}
}

func TestIntegrationExecution(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	name := "sx_cron_" + strings.ToLower(rand.Text())
	s, err := New(db, name)
	if err != nil {
		t.Fatal(err)
	}
	// Name is generated locally and quoted independently, never from request text.
	table := `"public"."` + name + `"`
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE "+table+" (occurred_at timestamptz DEFAULT clock_timestamp())"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := s.Unschedule(cleanup, "tick"); err != nil {
			t.Error(err)
		}
		if _, err := db.ExecContext(cleanup, "DROP TABLE "+table); err != nil {
			t.Error(err)
		}
	}()
	if _, err := s.Schedule(t.Context(), "tick", "1 second", "INSERT INTO "+table+" DEFAULT VALUES"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("pg_cron did not execute scheduled SQL", ctx.Err())
		case <-ticker.C:
		}
	}
}
