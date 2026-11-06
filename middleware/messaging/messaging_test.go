package messaging_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	messagingmiddleware "github.com/nostosx/servex-contrib/middleware/messaging"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/storage/idempotency"
)

func TestBulkheadRejectsNilLimiterAtAssembly(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("nil limiter did not panic")
		}
	}()
	messagingmiddleware.Bulkhead(nil)
}

func TestBulkheadAdmitsThenReleases(t *testing.T) {
	t.Parallel()
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	handle := messagingmiddleware.Bulkhead(limiter)(func(context.Context, *messaging.Message) error { return nil })
	if err := handle(t.Context(), &messaging.Message{ID: "m1"}); err != nil {
		t.Fatal(err)
	}
	if err := handle(t.Context(), &messaging.Message{ID: "m2"}); err != nil {
		t.Fatal(err)
	}
}

func TestIdentityAssignsStableIDAndPreservesTheCallerMessage(t *testing.T) {
	t.Parallel()
	var published []*messaging.Message
	publish := messagingmiddleware.Identity()(func(_ context.Context, message *messaging.Message) error {
		published = append(published, message)
		return nil
	})
	original := &messaging.Message{Topic: "t", Key: "partition"}
	if err := publish(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	if len(published) != 1 {
		t.Fatalf("published %d messages", len(published))
	}
	if published[0].ID == "" {
		t.Fatal("identity not assigned")
	}
	if original.ID != "" {
		t.Fatal("identity mutated the caller's message")
	}
	if published[0].Key != "partition" {
		t.Fatal("partitioning key lost")
	}
}

func TestIdentityKeepsAnExistingID(t *testing.T) {
	t.Parallel()
	var got string
	publish := messagingmiddleware.Identity()(func(_ context.Context, message *messaging.Message) error {
		got = message.ID
		return nil
	})
	if err := publish(t.Context(), &messaging.Message{ID: "given", Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if got != "given" {
		t.Fatalf("id = %q, want the caller's id", got)
	}
}

func TestIdentityRejectsNilMessage(t *testing.T) {
	t.Parallel()
	publish := messagingmiddleware.Identity()(func(context.Context, *messaging.Message) error { return nil })
	if err := publish(t.Context(), nil); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", errorx.CodeOf(err))
	}
}

func TestRecoveryClassifiesPanicAsInternal(t *testing.T) {
	t.Parallel()
	handle := messagingmiddleware.Recovery()(func(context.Context, *messaging.Message) error { panic("boom") })
	err := handle(t.Context(), &messaging.Message{ID: "m"})
	if errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("code = %v, want Internal", errorx.CodeOf(err))
	}
	if errorx.IsRetryable(err) {
		t.Fatal("recovered panic must not be retryable")
	}
}

func TestTimeoutInstallsDeadlineOnlyForPositiveDuration(t *testing.T) {
	t.Parallel()
	handle := messagingmiddleware.Timeout(0)(func(ctx context.Context, _ *messaging.Message) error {
		if _, ok := ctx.Deadline(); ok {
			t.Fatal("nonpositive duration installed a deadline")
		}
		return nil
	})
	if err := handle(t.Context(), &messaging.Message{}); err != nil {
		t.Fatal(err)
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
			messagingmiddleware.Deduplicate(nil, func(context.Context, *messaging.Message) (idempotency.Key, error) {
				return idempotency.Key{}, nil
			})
		},
		"nil key": func() { messagingmiddleware.Deduplicate(&recordingStore{}, nil) },
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

func TestDeduplicateRejectsMissingMessageIdentity(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handle := messagingmiddleware.Deduplicate(store, func(context.Context, *messaging.Message) (idempotency.Key, error) {
		return idempotency.Key{Scope: "s", Fingerprint: [32]byte{1}}, nil
	})(func(context.Context, *messaging.Message) error { return nil })
	if err := handle(t.Context(), &messaging.Message{Topic: "t"}); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", errorx.CodeOf(err))
	}
	// A consumer must not invent an identity to make deduplication pass.
	if store.calls != 0 {
		t.Fatal("store consulted without a message id")
	}
}

func TestDeduplicateUsesMessageIDAsToken(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handle := messagingmiddleware.Deduplicate(store, func(context.Context, *messaging.Message) (idempotency.Key, error) {
		return idempotency.Key{Scope: "tenant", Fingerprint: [32]byte{7}}, nil
	})(func(context.Context, *messaging.Message) error { return nil })
	if err := handle(t.Context(), &messaging.Message{ID: "stable", Topic: "orders"}); err != nil {
		t.Fatal(err)
	}
	if store.key.Token != "stable" || store.key.Operation != "consume:orders" || store.key.Scope != "tenant" {
		t.Fatalf("key = %+v", store.key)
	}
}

func TestDeduplicatePropagatesKeyDerivationError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("no tenant")
	store := &recordingStore{}
	handle := messagingmiddleware.Deduplicate(store, func(context.Context, *messaging.Message) (idempotency.Key, error) {
		return idempotency.Key{}, sentinel
	})(func(context.Context, *messaging.Message) error { return nil })
	err := handle(t.Context(), &messaging.Message{ID: "m", Topic: "t"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("cause lost: %v", err)
	}
	if store.calls != 0 {
		t.Fatal("store consulted after a key failure")
	}
}
