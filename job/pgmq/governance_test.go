package pgmq

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/nostosx/servex/job"
)

func TestObserverReportsCommitFailureInsteadOfCompletion(t *testing.T) {
	q, mock := fixture(t)
	cause := errors.New("commit failed")
	var event job.Event
	q.cfg.Observer = func(_ context.Context, got job.Event) { event = got }
	mock.ExpectBegin()
	mock.ExpectQuery("WITH locked").WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"read_ct", "live"}).AddRow(1, true))
	mock.ExpectQuery("SELECT pgmq.delete").WithArgs("jobs", int64(1)).WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	mock.ExpectExec("DELETE FROM").WithArgs("job", "id:job", "unique:").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(cause)
	err := q.settle(t.Context(), delivery{id: 1, receipt: 1}, envelope{Job: job.Job{ID: "job", Type: "refresh"}}, false, false, 0)
	if !errors.Is(err, cause) || event.Kind != "settlement_failed" || !errors.Is(event.Error, cause) {
		t.Fatalf("error=%v event=%+v", err, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
