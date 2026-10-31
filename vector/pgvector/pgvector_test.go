package pgvector

import (
	"database/sql"
	"math"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/nostosx/servex/storage/vector"
)

func store(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db, Config{Namespace: "tenant", Dimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	return s, mock
}
func TestValidationDoesNotWrite(t *testing.T) {
	s, mock := store(t)
	for _, records := range [][]vector.Record{{{ID: "one", Vector: []float32{1}}}, {{ID: "one", Vector: []float32{0, 0}}}, {{ID: "one", Vector: []float32{float32(math.NaN()), 1}}}, {{ID: "same", Vector: []float32{1, 0}}, {ID: "same", Vector: []float32{1, 0}}}} {
		if err := s.Upsert(t.Context(), records...); err == nil {
			t.Fatal("accepted invalid batch")
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestExactAndApproximateQueries(t *testing.T) {
	for _, approx := range []bool{false, true} {
		s, mock := store(t)
		pattern := "WITH candidates AS MATERIALIZED"
		if approx {
			pattern = "SELECT id,embedding"
		}
		mock.ExpectQuery(pattern).WithArgs("tenant", "[1,0]", "{}", 2).WillReturnRows(sqlmock.NewRows([]string{"id", "vector", "metadata", "payload", "distance"}).AddRow("one", "[1,0]", `{"kind":"doc"}`, []byte("body"), 0.0))
		hits, err := s.Search(t.Context(), vector.Query{Vector: []float32{1, 0}, Limit: 2, Approximate: approx})
		if err != nil || len(hits) != 1 || hits[0].Record.Metadata["kind"] != "doc" {
			t.Fatalf("%v: %v", hits, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestUpsertRollsBackWholeBatch(t *testing.T) {
	s, mock := store(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()
	err := s.Upsert(t.Context(), vector.Record{ID: "one", Vector: []float32{1, 0}}, vector.Record{ID: "two", Vector: []float32{0, 1}})
	if err == nil {
		t.Fatal("expected rollback")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestIndexInputCannotInjectSQL(t *testing.T) {
	s, mock := store(t)
	for _, kind := range []IndexKind{"hnsw; DROP TABLE x", IVFFlat} {
		if err := s.CreateIndex(t.Context(), "index", kind, 0); err == nil {
			t.Fatal("accepted invalid index")
		}
	}
	if err := s.CreateIndex(t.Context(), strings.Repeat("a", 64), HNSW, 0); err == nil {
		t.Fatal("accepted long name")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
