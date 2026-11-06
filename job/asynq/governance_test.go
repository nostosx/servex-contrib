package asynq

import (
	"context"
	"testing"

	native "github.com/hibiken/asynq"

	"github.com/nostosx/servex/job"
)

func TestAttemptObserverDoesNotClaimSDKSettlement(t *testing.T) {
	t.Parallel()
	var events []job.Event
	q := &Queue{opts: options{observer: func(_ context.Context, event job.Event) { events = append(events, event) }}}
	err := q.handler(t.Context(), map[string]job.Handler{"refresh": func(context.Context, *job.Job) error { return nil }})(t.Context(), native.NewTask("refresh", nil))
	if err != nil || len(events) != 1 || events[0].Kind != "attempt_finished" || events[0].Attempt != 1 {
		t.Fatalf("error=%v events=%v", err, events)
	}
}
