package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	jobmiddleware "github.com/nostosx/servex-contrib/middleware/job"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/storage/idempotency"
)

func TestBulkheadRejectsNilLimiterAtAssembly(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("nil limiter did not panic")
		}
	}()
	jobmiddleware.Bulkhead(nil)
}

func TestBulkheadAdmitsAndReleases(t *testing.T) {
	t.Parallel()
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	handle := jobmiddleware.Bulkhead(limiter)(func(context.Context, *job.Job) error { return nil })
	if err := handle(t.Context(), &job.Job{ID: "j1"}); err != nil {
		t.Fatal(err)
	}
	// The slot must be free again after a successful execution.
	if err := handle(t.Context(), &job.Job{ID: "j2"}); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutShortensButDoesNotReplaceTheHandler(t *testing.T) {
	t.Parallel()
	handle := jobmiddleware.Timeout(0)(func(ctx context.Context, _ *job.Job) error {
		if _, ok := ctx.Deadline(); ok {
			t.Fatal("nonpositive duration installed a deadline")
		}
		return nil
	})
	if err := handle(t.Context(), &job.Job{}); err != nil {
		t.Fatal(err)
	}
	handle = jobmiddleware.Timeout(30_000_000_000)(func(ctx context.Context, _ *job.Job) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("timeout installed no deadline")
		}
		return nil
	})
	if err := handle(t.Context(), &job.Job{}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryClassifiesPanicAsInternal(t *testing.T) {
	t.Parallel()
	handle := jobmiddleware.Recovery()(func(context.Context, *job.Job) error { panic("boom") })
	err := handle(t.Context(), &job.Job{})
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("code = %v, want Internal", errorx.CodeOf(err))
	}
	if errorx.IsRetryable(err) {
		t.Fatal("recovered panic must not be retryable")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cause: %v", err)
	}
}

type recordingStore struct {
	calls int
	key   idempotency.Key
}

func (s *recordingStore) Do(_ context.Context, key idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	s.calls++
	s.key = key
	return fn(context.Background())
}

func TestDeduplicateRequiresDependencies(t *testing.T) {
	t.Parallel()
	for name, build := range map[string]func(){
		"nil store": func() {
			jobmiddleware.Deduplicate(nil, func(context.Context, *job.Job) (idempotency.Key, error) { return idempotency.Key{}, nil })
		},
		"nil key": func() { jobmiddleware.Deduplicate(&recordingStore{}, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s did not panic", name)
				}
			}()
			build()
		}()
	}
}

func TestDeduplicateRejectsUnstableIdentity(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handle := jobmiddleware.Deduplicate(store, func(context.Context, *job.Job) (idempotency.Key, error) {
		return idempotency.Key{Scope: "s", Fingerprint: [32]byte{1}}, nil
	})(func(context.Context, *job.Job) error { return nil })
	if err := handle(t.Context(), &job.Job{Type: "t"}); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", errorx.CodeOf(err))
	}
	if store.calls != 0 {
		t.Fatal("store consulted without a stable identity")
	}
}

func TestDeduplicateUsesJobIDAsToken(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handle := jobmiddleware.Deduplicate(store, func(_ context.Context, _ *job.Job) (idempotency.Key, error) {
		return idempotency.Key{Scope: "tenant", Fingerprint: [32]byte{7}}, nil
	})(func(context.Context, *job.Job) error { return nil })
	if err := handle(t.Context(), &job.Job{ID: "stable", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	// The stable job ID is the idempotency token, so a retry of the same job
	// reuses the record instead of executing again.
	if store.key.Token != "stable" || store.key.Operation != "execute:t" || store.key.Scope != "tenant" {
		t.Fatalf("key = %+v", store.key)
	}
}
