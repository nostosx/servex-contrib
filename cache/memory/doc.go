// Package memory provides an in-process cache.Cache with LRU eviction and
// per-entry TTL. Expired entries are removed lazily on access and preferred
// for eviction; no background goroutine is started.
package memory
