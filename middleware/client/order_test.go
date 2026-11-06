package client_test

import (
	"context"
	"testing"

	clientmiddleware "github.com/nostosx/servex-contrib/middleware/client"
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
	handler := endpoint.Chain(clientmiddleware.Recommended(map[clientmiddleware.Policy]endpoint.Middleware{
		clientmiddleware.PolicyCircuitBreaker: marker(&entered, "circuit_breaker"),
		clientmiddleware.PolicyTracing:        marker(&entered, "tracing"),
		clientmiddleware.PolicyBulkhead:       marker(&entered, "bulkhead"),
		clientmiddleware.PolicyAuthentication: marker(&entered, "authentication"),
		clientmiddleware.PolicyLogging:        marker(&entered, "logging"),
		clientmiddleware.PolicyMetrics:        nil,
	})...)(func(context.Context, any) (any, error) { return "done", nil })

	if _, err := handler(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"tracing", "logging", "authentication", "bulkhead", "circuit_breaker"}
	if len(entered) != len(want) {
		t.Fatalf("entered %v, want %v", entered, want)
	}
	for index := range want {
		if entered[index] != want[index] {
			t.Fatalf("entered %v, want %v", entered, want)
		}
	}
}

func TestCanonicalOrderKeepsBulkheadOutsideBreaker(t *testing.T) {
	t.Parallel()
	// IsCircuitBreakerFailure counts ResourceExhausted as a backend failure, so a
	// bulkhead inside the breaker would let local overload open it.
	order := clientmiddleware.Order()
	bulkhead, breaker := -1, -1
	for index, policy := range order {
		switch policy {
		case clientmiddleware.PolicyBulkhead:
			bulkhead = index
		case clientmiddleware.PolicyCircuitBreaker:
			breaker = index
		}
	}
	if bulkhead < 0 || breaker < 0 {
		t.Fatalf("canonical order missing policies: %v", order)
	}
	if bulkhead > breaker {
		t.Fatalf("bulkhead sits inside the breaker: %v", order)
	}
	if err := clientmiddleware.Check(order...); err != nil {
		t.Fatalf("canonical order rejected: %v", err)
	}
}

func TestRecommendedRejectsUnknownPolicy(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("unknown policy did not panic")
		}
	}()
	clientmiddleware.Recommended(map[clientmiddleware.Policy]endpoint.Middleware{"nope": nil})
}

func TestCheckRejectsReversedAndRepeated(t *testing.T) {
	t.Parallel()
	if err := clientmiddleware.Check(clientmiddleware.PolicyCircuitBreaker, clientmiddleware.PolicyTracing); err == nil {
		t.Fatal("reversed order accepted")
	}
	// Bulkhead is canonical outside the breaker; putting it inside is the defect.
	if err := clientmiddleware.Check(clientmiddleware.PolicyCircuitBreaker, clientmiddleware.PolicyBulkhead); err == nil {
		t.Fatal("bulkhead inside the breaker accepted")
	}
	if err := clientmiddleware.Check(clientmiddleware.PolicyLogging, clientmiddleware.PolicyLogging); err == nil {
		t.Fatal("repeated policy accepted")
	}
	if err := clientmiddleware.Check("nope"); err == nil {
		t.Fatal("unknown policy accepted")
	}
}

func TestOrderReturnsACopy(t *testing.T) {
	t.Parallel()
	order := clientmiddleware.Order()
	order[0] = "tampered"
	if clientmiddleware.Order()[0] != clientmiddleware.PolicyTracing {
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
	clientmiddleware.Bulkhead(nil)
}
