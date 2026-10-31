package pgmq

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

func fixture(t *testing.T) (*Queue, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	q, err := New(db, Config{Queue: "jobs", VisibilityTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close(context.Background()); _ = db.Close() })
	return q, mock
}
func TestDuplicateRollsBackBeforeSend(t *testing.T) {
	q, mock := fixture(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WithArgs("id:id", "id", nil).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO").WithArgs("unique:one", "id", 0.25).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	_, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "id", Type: "task"}, jobqueue.WithUnique("one", 250*time.Millisecond))
	if !errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestEnqueueTxDoesNotCommit(t *testing.T) {
	q, mock := fixture(t)
	mock.ExpectBegin()
	tx, err := q.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT pgmq.send").WithArgs("jobs", sqlmock.AnyArg(), int64(2)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	id, err := q.EnqueueTx(t.Context(), tx, &jobqueue.Job{ID: "id", Type: "task"}, jobqueue.WithDelay(1500*time.Millisecond))
	if err != nil || id != "id" {
		t.Fatalf("%s: %v", id, err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestStaleReceiptCannotSettle(t *testing.T) {
	q, mock := fixture(t)
	mock.ExpectBegin()
	mock.ExpectQuery("WITH locked").WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"read_ct", "live"}).AddRow(2, true))
	mock.ExpectRollback()
	if err := q.settle(t.Context(), delivery{id: 1, receipt: 1}, envelope{}, false, false, 0); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestExpiredReceiptCannotBeRenewed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, mock := fixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mock.ExpectExec("WITH locked").WithArgs(int64(1), int64(1), int64(3)).WillReturnResult(sqlmock.NewResult(0, 0))
		result := make(chan error, 1)
		go func() { result <- q.heartbeat(ctx, delivery{id: 1, receipt: 1}, cancel) }()
		time.Sleep(time.Second)
		synctest.Wait()
		if err := <-result; !errors.Is(err, ErrLeaseLost) {
			t.Fatal(err)
		}
		if ctx.Err() == nil {
			t.Fatal("handler not canceled")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
func TestCloseCancelsAndWaitsWithoutClosingPool(t *testing.T) {
	q, mock := fixture(t)
	ctx, finish, err := q.operation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var finished atomic.Bool
	go func() { <-ctx.Done(); finished.Store(true); finish() }()
	if err := q.Close(t.Context()); err != nil || !finished.Load() {
		t.Fatalf("close: %v", err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "task"}); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := q.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal("borrowed pool closed", err)
	}
	_ = tx.Rollback()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestHandlerPanicBecomesPermanentFailure(t *testing.T) {
	err := call(t.Context(), func(context.Context, any) (any, error) { panic("private detail") }, &jobqueue.Job{})
	if errorx.CodeOf(err) != errorx.Internal || errorx.IsRetryable(err) {
		t.Fatal(err)
	}
}

func TestWorkerBacksOffOnTemporaryDatabaseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, mock := fixture(t)
		mock.ExpectQuery("SELECT msg_id").WillReturnError(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("offline")})
		permanent := errors.New("invalid schema")
		mock.ExpectQuery("SELECT msg_id").WillReturnError(permanent)
		result := make(chan error, 1)
		go func() { result <- q.work(t.Context(), nil) }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("temporary outage stopped worker: %v", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if err := <-result; !errors.Is(err, permanent) {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
