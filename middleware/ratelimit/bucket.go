package ratelimit

import (
	"math"
	"sync"
	"time"
)

// DefaultIdleTimeout is the default minimum idle time after which a bucket
// may be evicted.
const DefaultIdleTimeout = time.Minute

// BucketOption configures the Limiter returned by TokenBucket.
type BucketOption func(*bucketOptions)

type bucketOptions struct {
	now  func() time.Time
	idle time.Duration
}

// WithClock sets the clock used by the token bucket. The default is
// time.Now. A nil now keeps the default. It is intended for tests and must be
// safe for concurrent calls.
func WithClock(now func() time.Time) BucketOption {
	return func(o *bucketOptions) {
		if now != nil {
			o.now = now
		}
	}
}

// WithIdleTimeout sets the minimum time a bucket must stay unused before it
// may be evicted. The default is DefaultIdleTimeout; values <= 0 keep the
// default. The effective timeout is never shorter than the time an empty
// bucket needs to refill completely, so evicting a bucket never changes the
// outcome of later calls: a recreated bucket starts full, exactly like the
// evicted one would have been.
func WithIdleTimeout(d time.Duration) BucketOption {
	return func(o *bucketOptions) {
		if d > 0 {
			o.idle = d
		}
	}
}

// bucket is the state of the token bucket of one key.
type bucket struct {
	tokens float64
	last   time.Time
}

// tokenBucket is a keyed token-bucket Limiter.
type tokenBucket struct {
	rate  float64
	burst float64
	now   func() time.Time
	idle  time.Duration

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

var _ Limiter = (*tokenBucket)(nil)

// TokenBucket returns a Limiter that keeps one token bucket per key. Each
// bucket holds at most burst tokens, starts full and refills at rate tokens
// per second; a call is allowed when it can take one token.
//
// Buckets unused for the idle timeout (see WithIdleTimeout) are evicted
// lazily: Allow sweeps the buckets at most once per idle timeout. Idle buckets
// remain until a later Allow triggers a sweep; no background goroutine is
// started. There is no fixed limit on the number of keys retained.
//
// TokenBucket panics if rate is not a positive finite number or burst is
// less than 1.
func TokenBucket(rate float64, burst int, opts ...BucketOption) Limiter {
	if !(rate > 0) || math.IsInf(rate, 0) {
		panic("ratelimit: token bucket rate must be positive and finite")
	}
	if burst < 1 {
		panic("ratelimit: token bucket burst must be at least 1")
	}
	o := bucketOptions{now: time.Now, idle: DefaultIdleTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	// fill is the time an empty bucket needs to refill, rounded up so that a
	// bucket idle for fill is guaranteed to be full again.
	fill := time.Duration(math.MaxInt64)
	if ns := math.Ceil(float64(burst) / rate * float64(time.Second)); ns < float64(math.MaxInt64) {
		fill = time.Duration(ns)
	}
	tb := &tokenBucket{
		rate:    rate,
		burst:   float64(burst),
		now:     o.now,
		idle:    max(o.idle, fill),
		buckets: make(map[string]*bucket),
	}
	tb.lastSweep = tb.now()
	return tb
}

// Allow takes one token from the bucket of key if one is available.
func (tb *tokenBucket) Allow(key string) bool {
	now := tb.now()
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.sweep(now)
	b, ok := tb.buckets[key]
	if !ok {
		b = &bucket{tokens: tb.burst, last: now}
		tb.buckets[key] = b
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = min(tb.burst, b.tokens+elapsed.Seconds()*tb.rate)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweep evicts the buckets idle for at least the idle timeout. It does work
// at most once per idle timeout. tb.mu must be held.
func (tb *tokenBucket) sweep(now time.Time) {
	if now.Sub(tb.lastSweep) < tb.idle {
		return
	}
	tb.lastSweep = now
	for key, b := range tb.buckets {
		if now.Sub(b.last) >= tb.idle {
			delete(tb.buckets, key)
		}
	}
}
