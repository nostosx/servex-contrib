package client

import (
	"fmt"
	"slices"
	"strings"

	"github.com/nostosx/servex/endpoint"
)

// Policy names one slot in the canonical outbound middleware order. Outbound
// policies wrap a logical call, whose attempts and backoff come from
// transport/client.Policy, so their order decides what a local rejection is
// allowed to observe and pollute.
type Policy string

// Canonical outbound policies, outermost first.
const (
	// PolicyTracing starts the client span and injects the trace header.
	PolicyTracing Policy = "tracing"
	// PolicyLogging runs inside Tracing to carry trace/span IDs.
	PolicyLogging Policy = "logging"
	// PolicyMetrics has the same position rationale as PolicyLogging.
	PolicyMetrics Policy = "metrics"
	// PolicyAuthentication injects credentials outside the breaker, so an
	// unavailable token source never counts against the target's health.
	PolicyAuthentication Policy = "authentication"
	// PolicyBulkhead bounds local concurrency outside the breaker. It must stay
	// outside: IsCircuitBreakerFailure counts ResourceExhausted as a backend
	// failure, so an inner bulkhead would let local overload open the breaker.
	PolicyBulkhead Policy = "bulkhead"
	// PolicyCircuitBreaker is innermost and therefore observes only reached
	// attempts, never local authentication or admission rejections.
	PolicyCircuitBreaker Policy = "circuit_breaker"
)

// outboundOrder is the canonical outbound order, outermost first.
var outboundOrder = []Policy{
	PolicyTracing,
	PolicyLogging,
	PolicyMetrics,
	PolicyAuthentication,
	PolicyBulkhead,
	PolicyCircuitBreaker,
}

// Order returns the canonical outbound policy order, outermost first.
func Order() []Policy { return slices.Clone(outboundOrder) }

// Recommended returns the supplied middleware in canonical order. Absent and nil
// entries are omitted. An unknown policy panics during assembly rather than being
// silently dropped.
func Recommended(chain map[Policy]endpoint.Middleware) []endpoint.Middleware {
	unknown := make([]string, 0, len(chain))
	for policy := range chain {
		if !slices.Contains(outboundOrder, policy) {
			unknown = append(unknown, string(policy))
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		panic("client: unknown policy: " + strings.Join(unknown, ", "))
	}
	out := make([]endpoint.Middleware, 0, len(chain))
	for _, policy := range outboundOrder {
		if middleware := chain[policy]; middleware != nil {
			out = append(out, middleware)
		}
	}
	return out
}

// Check reports whether policies appear in canonical order, for callers that
// assemble a chain by hand. Repeating a policy or naming one outside the
// canonical set is an error.
func Check(policies ...Policy) error {
	seen := make(map[Policy]bool, len(policies))
	previousRank := -1
	previousPolicy := Policy("")
	for index, policy := range policies {
		rank := slices.Index(outboundOrder, policy)
		if rank < 0 {
			return fmt.Errorf("client: policy %d is unknown: %q", index, policy)
		}
		if seen[policy] {
			return fmt.Errorf("client: policy %q is repeated", policy)
		}
		seen[policy] = true
		if rank < previousRank {
			return fmt.Errorf("client: policy %q must come after %q", policy, previousPolicy)
		}
		previousRank, previousPolicy = rank, policy
	}
	return nil
}
