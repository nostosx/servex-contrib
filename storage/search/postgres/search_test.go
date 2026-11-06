package postgres

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/nostosx/servex/storage/search"
)

func TestEnginesBindQueryAndTenant(t *testing.T) {
	for _, engine := range []Engine{Native, ParadeDB, PGroonga, Trigram} {
		t.Run(string(engine), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s, err := New(db, Config{Namespace: "tenant", Engine: engine})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectQuery("SELECT id,body").WithArgs("tenant", "text' OR true --", `{"kind":"doc"}`, 3, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "body", "metadata", "payload", "score"}).AddRow("id", "body", `{"kind":"doc"}`, []byte("payload"), 2))
			hits, err := s.Search(t.Context(), search.Query{Text: "text' OR true --", Metadata: map[string]string{"kind": "doc"}, Limit: 3, Offset: 1})
			if err != nil || len(hits) != 1 {
				t.Fatalf("%v: %v", hits, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestBadDocumentsAndQueriesDoNotExecute(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(db, Config{Namespace: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(t.Context(), search.Document{ID: "id", Text: "a\x00b"}); err == nil {
		t.Fatal("accepted NUL")
	}
	if _, err := s.Search(t.Context(), search.Query{Text: "term", Limit: -1}); err == nil {
		t.Fatal("accepted invalid limit")
	}
	if err := s.CreateIndex(t.Context(), "index; DROP TABLE docs"); err == nil {
		t.Fatal("accepted index injection")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
