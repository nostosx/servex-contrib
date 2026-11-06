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

func TestApplicationValidationMatchesEndpointClassification(t *testing.T) {
	t.Parallel()
	for _, failure := range []error{nil, errors.New("invalid request"), errorx.New(errorx.OutOfRange, "LIMIT", "limit")} {
		calls := 0
		handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "order.create"}, application.Func[*request, int](func(context.Context, *request) (int, error) {
			calls++
			return 7, nil
		}), applicationmiddleware.Validation[*request, int](nil))
		req := &request{err: failure}
		result, err := handler.Handle(t.Context(), req)
		_, endpointErr := endpoint.Validation()(echo)(t.Context(), req)
		if errorx.CodeOf(err) != errorx.CodeOf(endpointErr) || (failure != nil && (!errors.Is(err, failure) || result != 0 || calls != 0)) || (failure == nil && (result != 7 || calls != 1)) {
			t.Fatalf("result=%d error=%v endpoint=%v calls=%d", result, err, endpointErr, calls)
		}
	}
}

func TestExplicitValidationReceivesCallerContext(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(context.Context, int) (int, error) { return 7, nil },
		applicationmiddleware.Validation[int, int](func(got context.Context, request int) error {
			if got != ctx || request != 3 {
				t.Fatal("validation lost context or request")
			}
			return nil
		}))
	if result, err := handler(ctx, 3); result != 7 || err != nil {
		t.Fatalf("result=%d error=%v", result, err)
	}
}

type request struct{ err error }

func (r *request) Validate() error                 { return r.err }
func echo(_ context.Context, req any) (any, error) { return req, nil }
