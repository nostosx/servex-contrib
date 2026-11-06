package observe_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestApplicationCompositionObservesRejectedCallAndPreservesTransport(t *testing.T) {
	r := &recording{}
	cause := errors.New("private authorization diagnostic")
	deny := applicationmw.Authorization[int, int](func(context.Context, application.Metadata, int) error {
		r.event("policy")
		return cause
	})
	policies := []application.Middleware[int, int]{applicationmw.Tracing[int, int](r), applicationmw.Logging[int, int](r), applicationmw.Metrics[int, int](meter{r}), deny}
	handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "order.create"}, application.Func[int, int](func(context.Context, int) (int, error) {
		t.Fatal("rejected call reached business")
		return 7, nil
	}), policies...)
	// Mutating the returned slice after assembly cannot change the call chain.
	policies[len(policies)-1] = nil
	info := metadata.Info{Kind: metadata.KindServer, Protocol: "http", Operation: "POST /orders", Header: metadata.Pairs("x-request", "original")}
	ctx := metadata.NewContext(t.Context(), info)
	result, err := handler.Handle(ctx, 1)
	want := []string{"span.start", "policy", "metric.count", "metric.duration", "log", "span.end"}
	if result != 0 || !errors.Is(err, cause) || !slices.Equal(r.events, want) || !slices.Equal(r.codes, []string{errorx.PermissionDenied.String()}) {
		t.Fatalf("result=%d error=%v events=%v codes=%v", result, err, r.events, r.codes)
	}
	got, _ := metadata.FromContext(ctx)
	if got.Kind != info.Kind || got.Protocol != info.Protocol || got.Operation != info.Operation || got.Header.Get("x-request") != "original" {
		t.Fatalf("application observation overwrote transport identity: %+v", got)
	}
}

func TestApplicationCompositionObservesAndPropagatesPanic(t *testing.T) {
	r := &recording{}
	marker := &struct{}{}
	fn := application.Wrap(application.Metadata{Kind: application.KindCall, Operation: "order.create"}, func(context.Context, int) (int, error) {
		panic(marker)
	}, applicationmw.Tracing[int, int](r), applicationmw.Logging[int, int](r), applicationmw.Metrics[int, int](meter{r}))
	defer func() {
		if got := recover(); got != marker || r.ended != 1 || !slices.Equal(r.codes, []string{errorx.Internal.String()}) || !slices.Contains(r.events, "metric.count") {
			t.Fatalf("panic=%v ended=%d codes=%v events=%v", got, r.ended, r.codes, r.events)
		}
	}()
	_, _ = fn(t.Context(), 1)
}
