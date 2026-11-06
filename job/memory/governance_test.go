package memory

import (
	"context"
	"slices"
	"testing"

	"github.com/nostosx/servex/job"
)

func TestRuntimeCompletionFollowsAttemptOutcome(t *testing.T) {
	t.Parallel()
	var events []string
	q := New(WithObserver(func(_ context.Context, event job.Event) {
		events = append(events, event.Kind)
		if event.JobID != "job" || event.Attempt != 1 {
			t.Fatalf("event=%+v", event)
		}
	}))
	defer q.Close()
	q.process(t.Context(), map[string]job.Handler{"refresh": func(context.Context, *job.Job) error { return nil }}, &entry{job: &job.Job{ID: "job", Type: "refresh", Attempt: 1}})
	if !slices.Equal(events, []string{"attempt_finished", "completed"}) {
		t.Fatalf("events=%v", events)
	}
}
