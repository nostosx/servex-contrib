package bulkhead_test

import (
	"context"
	"errors"
	"testing"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	"github.com/nostosx/servex-contrib/middleware/client"
	"github.com/nostosx/servex-contrib/middleware/endpoint"
	jobpolicy "github.com/nostosx/servex-contrib/middleware/job"
	messagepolicy "github.com/nostosx/servex-contrib/middleware/messaging"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/messaging"
)

func scopeCalls(limiter *bulkhead.Limiter, next func(context.Context) error) map[string]func(context.Context) error {
	endpointCall := endpoint.Bulkhead(limiter)(func(ctx context.Context, _ any) (any, error) { return nil, next(ctx) })
	clientCall := client.Bulkhead(limiter)(func(ctx context.Context, _ any) (any, error) { return nil, next(ctx) })
	applicationCall := application.Wrap(application.Metadata{Operation: "order.create"}, func(ctx context.Context, _ int) (int, error) { return 0, next(ctx) }, applicationmw.Bulkhead[int, int](limiter))
	messageCall := messagepolicy.Bulkhead(limiter)(func(ctx context.Context, _ *messaging.Message) error { return next(ctx) })
	jobCall := jobpolicy.Bulkhead(limiter)(func(ctx context.Context, _ *job.Job) error { return next(ctx) })
	return map[string]func(context.Context) error{
		"endpoint":    func(ctx context.Context) error { _, err := endpointCall(ctx, nil); return err },
		"client":      func(ctx context.Context) error { _, err := clientCall(ctx, nil); return err },
		"application": func(ctx context.Context) error { _, err := applicationCall(ctx, 3); return err },
		"messaging":   func(ctx context.Context) error { return messageCall(ctx, &messaging.Message{ID: "m"}) },
		"job":         func(ctx context.Context) error { return jobCall(ctx, &job.Job{ID: "j"}) },
	}
}

func TestScopesShareAdmissionAndPreserveErrors(t *testing.T) {
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("business failure")
	calls := 0
	for name, call := range scopeCalls(limiter, func(context.Context) error { calls++; return failure }) {
		t.Run(name, func(t *testing.T) {
			release, err := limiter.Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			before := calls
			blocked := call(t.Context())
			release()
			if errorx.CodeOf(blocked) != errorx.ResourceExhausted || calls != before {
				t.Fatalf("blocked=%v calls=%d", blocked, calls-before)
			}
			if got := call(t.Context()); got != failure {
				t.Fatalf("business error changed: %v", got)
			}
			release, err = limiter.Acquire(t.Context())
			if err != nil {
				t.Fatalf("failure leaked slot: %v", err)
			}
			release()
		})
	}
}

func TestScopesReleaseSlotsWhenPanicking(t *testing.T) {
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("panic identity")
	for name, call := range scopeCalls(limiter, func(context.Context) error { panic(failure) }) {
		t.Run(name, func(t *testing.T) {
			func() {
				defer func() {
					if got := recover(); got != failure {
						t.Fatalf("panic=%v", got)
					}
				}()
				_ = call(t.Context())
			}()
			release, err := limiter.Acquire(t.Context())
			if err != nil {
				t.Fatalf("panic leaked slot: %v", err)
			}
			release()
		})
	}
}

func TestAdmissionRejectsCanceledCallerWithoutSpendingSlot(t *testing.T) {
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("caller stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	if release, err := limiter.Acquire(ctx); release != nil || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("release=%v error=%v", release != nil, err)
	}
	release, err := limiter.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	release()
}
