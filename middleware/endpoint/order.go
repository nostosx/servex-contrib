package endpoint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/nostosx/servex/endpoint"
)

// Policy names one slot in the canonical inbound middleware order. Hand-wrapping
// these policies in a different order is legal but changes their meaning: the
// first wrapper is outermost, so a policy placed outside Recovery observes a
// panic instead of a classified error, and one placed inside Budget spends the
// caller's deadline while it waits.
type Policy string

// Canonical inbound policies, outermost first.
const (
	// PolicyTracing starts the server span, so rejected calls are still traced.
	PolicyTracing Policy = "tracing"
	// PolicyLogging runs inside Tracing to carry trace/span IDs, and outside
	// Recovery so a recovered panic is recorded as a classified outcome.
	PolicyLogging Policy = "logging"
	// PolicyMetrics has the same position rationale as PolicyLogging.
	PolicyMetrics Policy = "metrics"
	// PolicyRecovery runs outside every remaining policy so any panic becomes a
	// non-retryable Internal error before admission or authentication runs.
	PolicyRecovery Policy = "recovery"
	// PolicyBudget bounds all downstream work, including remote admission and
	// verification calls. It never extends an earlier caller deadline.
	PolicyBudget Policy = "budget"
	// PolicyRateLimit rejects over-quota calls before a concurrency slot is spent.
	PolicyRateLimit Policy = "rate_limit"
	// PolicyBulkhead bounds concurrent calls before the cost of authentication.
	PolicyBulkhead Policy = "bulkhead"
	// PolicyAuthentication establishes the principal for downstream policies.
	PolicyAuthentication Policy = "authentication"
	// PolicyValidation checks the request closest to the handler.
	PolicyValidation Policy = "validation"
)

// inboundOrder is the canonical inbound order, outermost first.
var inboundOrder = []Policy{
	PolicyTracing,
	PolicyLogging,
	PolicyMetrics,
	PolicyRecovery,
	PolicyBudget,
	PolicyRateLimit,
	PolicyBulkhead,
	PolicyAuthentication,
	PolicyValidation,
}

// Order returns the canonical inbound policy order, outermost first.
func Order() []Policy { return slices.Clone(inboundOrder) }

// Recommended returns the supplied middleware in canonical order. Absent and nil
// entries are omitted, so a caller can declare every slot and leave unused
// policies unset. An unknown policy panics during assembly rather than being
// silently dropped, matching the package's other configuration checks.
func Recommended(chain map[Policy]endpoint.Middleware) []endpoint.Middleware {
	unknown := make([]string, 0, len(chain))
	for policy := range chain {
		if !slices.Contains(inboundOrder, policy) {
			unknown = append(unknown, string(policy))
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		panic("endpoint: unknown policy: " + strings.Join(unknown, ", "))
	}
	out := make([]endpoint.Middleware, 0, len(chain))
	for _, policy := range inboundOrder {
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
		rank := slices.Index(inboundOrder, policy)
		if rank < 0 {
			return fmt.Errorf("endpoint: policy %d is unknown: %q", index, policy)
		}
		if seen[policy] {
			return fmt.Errorf("endpoint: policy %q is repeated", policy)
		}
		seen[policy] = true
		if rank < previousRank {
			return fmt.Errorf("endpoint: policy %q must come after %q", policy, previousPolicy)
		}
		previousRank, previousPolicy = rank, policy
	}
	return nil
}
