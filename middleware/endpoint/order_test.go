package endpoint_test

import (
	"context"
	"testing"

	endpointmiddleware "github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/endpoint"
)

func marker(log *[]string, name string) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			*log = append(*log, name)
			return next(ctx, request)
		}
	}
}

func TestRecommendedInstallsCanonicalOrder(t *testing.T) {
	t.Parallel()
	var entered []string
	handler := endpoint.Chain(endpointmiddleware.Recommended(map[endpointmiddleware.Policy]endpoint.Middleware{
		// Declared out of order on purpose; Recommended must reorder them.
		endpointmiddleware.PolicyValidation:     marker(&entered, "validation"),
		endpointmiddleware.PolicyTracing:        marker(&entered, "tracing"),
		endpointmiddleware.PolicyAuthentication: marker(&entered, "authentication"),
		endpointmiddleware.PolicyLogging:        marker(&entered, "logging"),
		endpointmiddleware.PolicyBulkhead:       nil,
	})...)(func(context.Context, any) (any, error) { return "done", nil })

	if _, err := handler(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"tracing", "logging", "authentication", "validation"}
	if len(entered) != len(want) {
		t.Fatalf("entered %v, want %v", entered, want)
	}
	for index := range want {
		if entered[index] != want[index] {
			t.Fatalf("entered %v, want %v", entered, want)
		}
	}
}

func TestRecommendedRejectsUnknownPolicy(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("unknown policy did not panic")
		}
	}()
	endpointmiddleware.Recommended(map[endpointmiddleware.Policy]endpoint.Middleware{"nope": nil})
}

func TestCheckAcceptsCanonicalOrder(t *testing.T) {
	t.Parallel()
	policies := endpointmiddleware.Order()
	if err := endpointmiddleware.Check(policies...); err != nil {
		t.Fatalf("canonical order rejected: %v", err)
	}
	if err := endpointmiddleware.Check(endpointmiddleware.PolicyTracing, endpointmiddleware.PolicyValidation); err != nil {
		t.Fatalf("partial canonical order rejected: %v", err)
	}
}

func TestCheckRejectsReversedOrder(t *testing.T) {
	t.Parallel()
	if err := endpointmiddleware.Check(endpointmiddleware.PolicyValidation, endpointmiddleware.PolicyTracing); err == nil {
		t.Fatal("reversed order accepted")
	}
	// The safety-relevant case: Recovery outside Logging hides a recovered panic
	// from the completion log.
	if err := endpointmiddleware.Check(endpointmiddleware.PolicyRecovery, endpointmiddleware.PolicyLogging); err == nil {
		t.Fatal("recovery outside logging accepted")
	}
}

func TestCheckRejectsRepeatedAndUnknown(t *testing.T) {
	t.Parallel()
	if err := endpointmiddleware.Check(endpointmiddleware.PolicyLogging, endpointmiddleware.PolicyLogging); err == nil {
		t.Fatal("repeated policy accepted")
	}
	if err := endpointmiddleware.Check("nope"); err == nil {
		t.Fatal("unknown policy accepted")
	}
}

func TestOrderReturnsACopy(t *testing.T) {
	t.Parallel()
	order := endpointmiddleware.Order()
	order[0] = "tampered"
	if endpointmiddleware.Order()[0] != endpointmiddleware.PolicyTracing {
		t.Fatal("Order exposed the canonical slice")
	}
}

func TestBulkheadRejectsNilLimiterAtAssembly(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("nil limiter did not panic")
		}
	}()
	endpointmiddleware.Bulkhead(nil)
}

func TestAuthenticationRejectsNilVerifierAtAssembly(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("nil verifier did not panic")
		}
	}()
	endpointmiddleware.Authentication[int](nil)
}
