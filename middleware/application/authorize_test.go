package application_test

import (
	"context"
	"errors"
	"testing"

	applicationmiddleware "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex-contrib/middleware/endpoint"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
)

func TestPolicyUsesAuthenticatedContextAndRejectsWithoutCallingBusiness(t *testing.T) {
	t.Parallel()
	cause := errors.New("private policy diagnostic")
	ctx := endpoint.NewPrincipalContext(t.Context(), "owner")
	calls := 0
	base := application.Func[string, string](func(context.Context, string) (string, error) {
		calls++
		return "created", nil
	})
	policy := applicationmiddleware.Authorization[string, string](func(ctx context.Context, meta application.Metadata, claimedOwner string) error {
		principal, ok := endpoint.PrincipalFromContext[string](ctx)
		if meta.Kind != application.KindCommand || meta.Operation != "order.create" {
			t.Fatal(meta)
		}
		if !ok || principal != claimedOwner {
			return cause
		}
		return nil
	})
	handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "order.create"}, base, policy)
	if got, err := handler.Handle(ctx, "owner"); got != "created" || err != nil {
		t.Fatalf("authorized call: %q %v", got, err)
	}
	got, err := handler.Handle(ctx, "other")
	classified := errorx.From(err)
	if got != "" || calls != 1 || !errors.Is(err, cause) || classified.Code != errorx.PermissionDenied || classified.Message != "permission denied" {
		t.Fatalf("unauthorized result=%q calls=%d error=%v", got, calls, err)
	}
}

func TestClassifiedFailureAndNilPolicy(t *testing.T) {
	t.Parallel()
	failure := errorx.New(errorx.Unavailable, "POLICY_UNAVAILABLE", "policy unavailable")
	handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(context.Context, int) (int, error) {
		t.Fatal("denied request reached business")
		return 1, nil
	}, applicationmiddleware.Authorization[int, int](func(context.Context, application.Metadata, int) error { return failure }))
	if result, err := handler(t.Context(), 1); result != 0 || err != failure {
		t.Fatalf("result/error changed: %d %v", result, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("nil policy must not silently allow access")
		}
	}()
	applicationmiddleware.Authorization[int, int](nil)
}
