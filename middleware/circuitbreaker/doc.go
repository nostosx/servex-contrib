// Package circuitbreaker provides client middleware implementing the
// adaptive throttling algorithm described in the Google SRE book ("Handling
// Overload"): each breaker tracks requests and accepted requests over a
// rolling window and rejects new calls locally with probability
//
//	max(0, (requests - K*accepts) / (requests + 1))
//
// Rejected calls fail with an errorx.Unavailable error with reason
// "CIRCUIT_OPEN" that is explicitly not retryable, so an outer retry stops
// instead of hammering an overloaded backend.
package circuitbreaker
