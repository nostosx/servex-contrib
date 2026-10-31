package pgcron

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNamedSchedulesAreBoundAndScoped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(db, "app")
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT cron.schedule").WithArgs("app:refresh", "* * * * *", "SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(42))
	if id, err := s.Schedule(t.Context(), "refresh", "* * * * *", "SELECT 1"); err != nil || id != 42 {
		t.Fatalf("%d: %v", id, err)
	}
	mock.ExpectQuery("SELECT COALESCE").WithArgs("app:refresh").WillReturnRows(sqlmock.NewRows([]string{"removed"}).AddRow(false))
	if removed, err := s.Unschedule(t.Context(), "refresh"); err != nil || removed {
		t.Fatalf("%v: %v", removed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestQualifiedNameLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := New(db, strings.Repeat("p", 62)); err == nil {
		t.Fatal("accepted prefix without room for a name")
	}
	s, err := New(db, strings.Repeat("p", 61))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT cron.schedule").WithArgs(strings.Repeat("p", 61)+":a", "* * * * *", "SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	if _, err := s.Schedule(t.Context(), "a", "* * * * *", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"aa", "ab"} {
		if _, err := s.Schedule(t.Context(), name, "* * * * *", "SELECT 1"); err == nil {
			t.Fatal("accepted truncated job name")
		}
		if _, err := s.Unschedule(t.Context(), name); err == nil {
			t.Fatal("accepted truncated unschedule name")
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
