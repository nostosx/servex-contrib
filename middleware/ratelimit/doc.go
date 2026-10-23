// Package ratelimit provides rate limiting middleware backed by a Limiter,
// together with a keyed token-bucket Limiter whose idle buckets are evicted
// lazily, without a background goroutine.
package ratelimit
