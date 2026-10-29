// Package rate provides keyed local rate limits using golang.org/x/time/rate.
package rate

import (
	"math"
	"sync"
	"time"

	xrate "golang.org/x/time/rate"

	"github.com/nostosx/servex/errorx"
)

const (
	// DefaultMaxKeys bounds the number of retained independent policies.
	DefaultMaxKeys = 10000
	// DefaultIdleTimeout is the minimum idle period before a full bucket is reclaimed.
	DefaultIdleTimeout = time.Minute
)

// Option configures a Limiter.
type Option func(*options)

type options struct {
	maxKeys int
	idle    time.Duration
	now     func() time.Time
}

// WithMaxKeys sets the positive maximum number of retained keys. When full,
// unknown keys are rejected; existing buckets are never evicted to admit them.
func WithMaxKeys(n int) Option { return func(o *options) { o.maxKeys = n } }

// WithIdleTimeout sets a positive minimum idle period and sweep interval.
// Only full buckets are reclaimed, so expiry never restores spent quota early.
func WithIdleTimeout(d time.Duration) Option { return func(o *options) { o.idle = d } }

// WithClock sets the clock, mainly for tests; nil keeps time.Now. It runs under
// the limiter's lock and must return promptly without reentering the limiter.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

type bucket struct {
	limiter  *xrate.Limiter
	lastSeen time.Time
}

// Limiter implements the structural Allow(string) bool contract consumed by
// middleware/ratelimit. It starts no goroutines and needs no Close. Calls are
// safe for concurrent use. A nil or zero-value Limiter rejects every call.
// A Limiter must not be copied after first use.
type Limiter struct {
	rate    xrate.Limit
	burst   int
	opts    options
	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

// New creates an independent bucket per key, initially full, granting limit
// tokens per second with the given burst. Invalid rate, burst or options return
// InvalidArgument. Limits are local to this instance and are not shared across
// replicas. Allow never waits or reserves future tokens.
func New(limit float64, burst int, opts ...Option) (*Limiter, error) {
	o := options{maxKeys: DefaultMaxKeys, idle: DefaultIdleTimeout, now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if limit <= 0 || math.IsNaN(limit) || math.IsInf(limit, 0) || burst < 1 || o.maxKeys < 1 || o.idle <= 0 {
		return nil, errorx.New(errorx.InvalidArgument, "RATE_LIMIT_CONFIG", "rate limiter: rate, burst, max keys and idle timeout must be positive and finite")
	}
	return &Limiter{rate: xrate.Limit(limit), burst: burst, opts: o, buckets: make(map[string]*bucket), swept: o.now()}, nil
}

// Allow spends one available token for key. Unknown keys are rejected when
// the key bound is reached. Allow lazily reclaims idle, fully replenished
// buckets at most once per idle timeout; rejected attempts count as activity.
func (l *Limiter) Allow(key string) bool {
	if l == nil || l.buckets == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.opts.now()
	if now.Sub(l.swept) >= l.opts.idle {
		for key, b := range l.buckets {
			if now.Sub(b.lastSeen) >= l.opts.idle && b.limiter.TokensAt(now) >= float64(l.burst) {
				delete(l.buckets, key)
			}
		}
		l.swept = now
	}
	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= l.opts.maxKeys {
			return false
		}
		b = &bucket{limiter: xrate.NewLimiter(l.rate, l.burst)}
		l.buckets[key] = b
	}
	b.lastSeen = now
	return b.limiter.AllowN(now, 1)
}
