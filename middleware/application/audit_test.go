package application_test

import (
	"context"
	"errors"
	"testing"

	applicationmiddleware "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
)

func TestAuditFailureFailsClosedAndPreservesBusinessCause(t *testing.T) {
	t.Parallel()
	business, recording := errors.New("business failure"), errors.New("recording failed")
	seen := 0
	fn := application.Wrap(application.Metadata{Operation: "order.create"}, func(context.Context, int) (int, error) { return 7, business }, applicationmiddleware.Audit[int, int](applicationmiddleware.AuditRecorderFunc(func(_ context.Context, event applicationmiddleware.AuditEvent) error {
		seen++
		if event.Metadata.Operation != "order.create" || event.Code != errorx.Unknown || event.Interrupted {
			t.Fatalf("event=%+v", event)
		}
		return recording
	})))
	result, err := fn(t.Context(), 1)
	if result != 0 || !errors.Is(err, business) || !errors.Is(err, recording) || seen != 1 {
		t.Fatalf("result=%d error=%v count=%d", result, err, seen)
	}
}
