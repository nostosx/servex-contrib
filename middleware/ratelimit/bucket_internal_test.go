package ratelimit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// len returns the number of live buckets.
func (tb *tokenBucket) len() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return len(tb.buckets)
}

func TestEvictsIdleBuckets(t *testing.T) {
	clock := newFakeClock()
	tb := TokenBucket(10, 5, WithClock(clock.Now), WithIdleTimeout(time.Minute)).(*tokenBucket)

	for i := range 100 {
		tb.Allow("k" + strconv.Itoa(i))
	}
	if n := tb.len(); n != 100 {
		t.Fatalf("buckets = %d, want 100", n)
	}

	clock.Advance(30 * time.Second)
	tb.Allow("hot")
	if n := tb.len(); n != 101 {
		t.Fatalf("buckets = %d before the idle timeout, want 101", n)
	}

	clock.Advance(30 * time.Second)
	tb.Allow("hot")
	if n := tb.len(); n != 1 {
		t.Fatalf("buckets = %d after the idle timeout, want only the hot one", n)
	}
}

func TestSweepIsRateLimited(t *testing.T) {
	clock := newFakeClock()
	tb := TokenBucket(10, 5, WithClock(clock.Now), WithIdleTimeout(time.Minute)).(*tokenBucket)

	clock.Advance(59 * time.Second)
	tb.Allow("a") // no sweep yet: lastSweep was at construction
	clock.Advance(2 * time.Second)
	tb.Allow("b") // sweep at t=61s: "a" idle for 2s survives
	clock.Advance(59 * time.Second)
	tb.Allow("c") // t=120s, only 59s since the last sweep: nothing evicted
	if n := tb.len(); n != 3 {
		t.Fatalf("buckets = %d, want 3", n)
	}
	clock.Advance(time.Second)
	tb.Allow("c") // t=121s: sweep evicts "a" (idle 61s) and "b" (idle 60s)
	if n := tb.len(); n != 1 {
		t.Fatalf("buckets = %d, want 1", n)
	}
}

func TestIdleTimeoutNotShorterThanRefill(t *testing.T) {
	clock := newFakeClock()
	// An empty bucket needs 100s to refill 10 tokens at 0.1 tokens/s.
	tb := TokenBucket(0.1, 10, WithClock(clock.Now), WithIdleTimeout(time.Second)).(*tokenBucket)
	if tb.idle != 100*time.Second {
		t.Fatalf("idle = %v, want the refill time 100s", tb.idle)
	}
	for range 10 {
		tb.Allow("k")
	}
	if tb.Allow("k") {
		t.Fatal("bucket should be empty")
	}
	clock.Advance(50 * time.Second)
	tb.Allow("other") // would evict "k" with a 1s idle timeout
	if tb.len() != 2 {
		t.Fatal("a bucket that is not yet full was evicted")
	}
	// Only 5 tokens have been refilled for "k".
	allowed := 0
	for range 10 {
		if tb.Allow("k") {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed = %d after 50s, want 5", allowed)
	}
}

func TestDefaults(t *testing.T) {
	tb := TokenBucket(1000, 1, WithClock(nil), WithIdleTimeout(0), nil).(*tokenBucket)
	if tb.idle != DefaultIdleTimeout {
		t.Fatalf("idle = %v, want %v", tb.idle, DefaultIdleTimeout)
	}
	if tb.now == nil {
		t.Fatal("nil clock")
	}
	slow := TokenBucket(1e-300, 1).(*tokenBucket)
	if slow.idle <= 0 {
		t.Fatalf("idle overflowed: %v", slow.idle)
	}
}
