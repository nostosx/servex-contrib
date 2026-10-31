package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/nostosx/servex/errorx"
)

func TestIdentifiers(t *testing.T) {
	for _, parts := range [][]string{nil, {""}, {"public", "x;DROP TABLE x"}, {"x.y"}, {"UPPER"}} {
		if _, err := Ident(parts...); err == nil {
			t.Fatalf("accepted %q", parts)
		}
	}
	got, err := Ident("public", "documents")
	if err != nil || got != `"public"."documents"` {
		t.Fatalf("%s: %v", got, err)
	}
}

func TestLabelsPreserveExactStrings(t *testing.T) {
	for _, labels := range []map[string]string{{"key": "\xff"}, {"\x00": "value"}, {"key": "\x00"}} {
		if _, err := Labels(labels); err == nil {
			t.Fatal("accepted unrepresentable label")
		}
	}
	if got, err := Labels(map[string]string{"键": "值"}); err != nil || got != `{"键":"值"}` {
		t.Fatalf("%s: %v", got, err)
	}
}

type state string

func (s state) Error() string    { return "fixture" }
func (s state) SQLState() string { return string(s) }
func TestErrorClassification(t *testing.T) {
	for _, tt := range []struct {
		state state
		code  errorx.Code
	}{{"40001", errorx.Aborted}, {"23505", errorx.AlreadyExists}, {"08006", errorx.Unavailable}, {"42501", errorx.PermissionDenied}, {"42883", errorx.FailedPrecondition}, {"22000", errorx.InvalidArgument}, {"XX000", errorx.Unknown}} {
		err := Error(tt.state)
		if errorx.CodeOf(err) != tt.code || !errors.Is(err, tt.state) {
			t.Fatalf("%s: %v", tt.state, err)
		}
	}
	if !errors.Is(Error(context.Canceled), context.Canceled) {
		t.Fatal("cancellation identity lost")
	}
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	if err := Error(dial); errorx.CodeOf(err) != errorx.Unavailable || !errors.Is(err, dial) {
		t.Fatal(err)
	}
	if err := Error(sql.ErrConnDone); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatal(err)
	}
}
func TestTransactionRollbackAndPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectRollback()
	sentinel := errors.New("business failure")
	if err := Transaction(t.Context(), db, func(_ *sql.Tx) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	func() {
		defer func() {
			if recover() != "panic" {
				t.Error("panic replaced")
			}
		}()
		_ = Transaction(t.Context(), db, func(_ *sql.Tx) error { panic("panic") })
	}()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestExtensionsDoNotInstallImplicitly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT EXISTS").WithArgs("vector").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	if err := Require(t.Context(), db, "vector"); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatal(err)
	}
	if err := c.Enable(t.Context(), "bad; extension"); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
