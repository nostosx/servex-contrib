package circuitbreaker

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// ReasonOpen is the errorx reason of the error returned for rejected calls.
const ReasonOpen = "CIRCUIT_OPEN"

// ErrOpen is returned for calls rejected by the breaker. Its code is
// errorx.Unavailable, its reason ReasonOpen, and it is explicitly marked as
// not retryable. Use errors.Is(err, ErrOpen) to detect it.
var ErrOpen = errorx.New(errorx.Unavailable, ReasonOpen, "circuitbreaker: circuit open").
	WithRetryable(false)

// Defaults used by New.
const (
	// DefaultK is the default accepts multiplier.
	DefaultK = 1.5
	// DefaultWindow is the default length of the rolling window.
	DefaultWindow = 10 * time.Second
	// DefaultBuckets is the default number of buckets in the window.
	DefaultBuckets = 40
	// DefaultMinRequests is the default number of requests in the window
	// below which no call is rejected.
	DefaultMinRequests = 100
)

// Option configures the middleware returned by New.
type Option func(*options)

type options struct {
	key         func(ctx context.Context) string
	k           float64
	window      time.Duration
	buckets     int
	minRequests int64
	now         func() time.Time
	random      func() float64
}

// WithKey sets the function that maps a call to its breaker. Calls with the
// same key share one breaker. The default is the Operation of the call's
// metadata.Info, or "" when there is none; nil keeps the default.
// fn must be safe for concurrent calls. Breakers are retained for the lifetime
// of the middleware, so keys should come from a bounded set, such as route names.
func WithKey(fn func(ctx context.Context) string) Option {
	return func(o *options) {
		if fn != nil {
			o.key = fn
		}
	}
}

// WithK sets the accepts multiplier K. Lower values throttle more
// aggressively; the SRE book suggests 2 and the default is DefaultK.
// Non-positive values keep the default.
func WithK(k float64) Option {
	return func(o *options) {
		if k > 0 {
			o.k = k
		}
	}
}

// WithWindow sets the length of the rolling window over which requests and
// accepts are counted. The default is DefaultWindow; non-positive values
// keep the default.
func WithWindow(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.window = d
		}
	}
}

// WithBuckets sets the number of buckets the window is divided into. More
// buckets let old outcomes expire more smoothly. The default is
// DefaultBuckets; non-positive values keep the default.
func WithBuckets(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.buckets = n
		}
	}
}

// WithMinRequests sets the number of requests a breaker must have counted in
// its window before it starts rejecting calls. The default is
// DefaultMinRequests; negative values are treated as 0.
func WithMinRequests(n int) Option {
	return func(o *options) { o.minRequests = int64(max(n, 0)) }
}

// WithClock sets the clock used for the rolling window, mainly for tests.
// The default is time.Now; nil keeps the default. now must be safe for
// concurrent calls.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// WithRandom sets the source of uniformly distributed numbers in [0, 1) used
// to decide rejections, mainly for tests. It must be safe for concurrent
// use. The default is rand.Float64 from math/rand/v2; nil keeps the default.
func WithRandom(fn func() float64) Option {
	return func(o *options) {
		if fn != nil {
			o.random = fn
		}
	}
}

// New returns middleware that throttles calls with one adaptive breaker per
// key.
//
// A call is rejected with ErrOpen, without calling the next endpoint, with
// probability max(0, (requests - K*accepts) / (requests + 1)), where requests
// counts every call seen in the window, rejected ones included, and accepts
// counts the calls the backend handled. Nothing is rejected while requests is
// below the minimum set by WithMinRequests.
//
// The outcome of a call that reaches the next endpoint is classified by its
// errorx code: Unavailable, DeadlineExceeded, Internal, Unknown and
// ResourceExhausted count as failures; success and every other code, such as
// InvalidArgument or NotFound, show that the backend is serving and count as
// accepts.
func New(opts ...Option) endpoint.Middleware {
	o := options{
		key:         operation,
		k:           DefaultK,
		window:      DefaultWindow,
		buckets:     DefaultBuckets,
		minRequests: DefaultMinRequests,
		now:         time.Now,
		random:      rand.Float64,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	g := &group{opts: o, start: o.now(), width: max(o.window/time.Duration(o.buckets), 1)}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			b := g.get(o.key(ctx))
			if !b.allow(g.slot()) {
				return nil, ErrOpen
			}
			resp, err := next(ctx, req)
			b.record(g.slot(), !IsFailure(err))
			return resp, err
		}
	}
}

// IsFailure reports whether err counts as a backend failure for the breaker:
// its errorx code is Unavailable, DeadlineExceeded, Internal, Unknown or
// ResourceExhausted. A nil err is not a failure.
func IsFailure(err error) bool {
	switch errorx.CodeOf(err) {
	case errorx.Unavailable, errorx.DeadlineExceeded, errorx.Internal, errorx.Unknown, errorx.ResourceExhausted:
		return true
	default:
		return false
	}
}

// operation is the default key: the Operation of the call's metadata.Info.
func operation(ctx context.Context) string {
	info, _ := metadata.FromContext(ctx)
	return info.Operation
}

// group holds the breakers of one middleware instance, keyed by WithKey.
type group struct {
	opts     options
	start    time.Time     // origin of the bucket slots
	width    time.Duration // time covered by one bucket
	breakers sync.Map      // string -> *breaker
}

// get returns the breaker for key, creating it on first use.
func (g *group) get(key string) *breaker {
	if b, ok := g.breakers.Load(key); ok {
		return b.(*breaker)
	}
	b, _ := g.breakers.LoadOrStore(key, &breaker{
		opts:    &g.opts,
		buckets: make([]bucket, g.opts.buckets),
	})
	return b.(*breaker)
}

// slot returns the index of the bucket covering the current time.
func (g *group) slot() int64 {
	return max(int64(g.opts.now().Sub(g.start)/g.width), 0)
}

// bucket counts the outcomes of one time slot.
type bucket struct {
	slot     int64
	requests int64
	accepts  int64
}

// breaker is the adaptive throttle of one key. Its buckets form a ring: the
// bucket for slot s is buckets[s%len(buckets)] and is reset when it is
// reused for a newer slot.
type breaker struct {
	opts *options

	mu      sync.Mutex
	buckets []bucket
}

// allow decides whether a call in slot may proceed. A rejected call is
// counted as a request without an accept.
func (b *breaker) allow(slot int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	requests, accepts := b.sum(slot)
	if requests < b.opts.minRequests {
		return true
	}
	p := (float64(requests) - b.opts.k*float64(accepts)) / float64(requests+1)
	if p <= 0 || b.opts.random() >= p {
		return true
	}
	b.add(slot, false)
	return false
}

// record counts the outcome of a call that reached the backend.
func (b *breaker) record(slot int64, accepted bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.add(slot, accepted)
}

// add counts one request, and one accept when accepted, in slot. b.mu must
// be held.
func (b *breaker) add(slot int64, accepted bool) {
	bk := &b.buckets[slot%int64(len(b.buckets))]
	if bk.slot != slot {
		*bk = bucket{slot: slot}
	}
	bk.requests++
	if accepted {
		bk.accepts++
	}
}

// sum returns the requests and accepts counted in the window ending at slot.
// b.mu must be held.
func (b *breaker) sum(slot int64) (requests, accepts int64) {
	oldest := slot - int64(len(b.buckets))
	for _, bk := range b.buckets {
		if bk.slot > oldest && bk.slot <= slot {
			requests += bk.requests
			accepts += bk.accepts
		}
	}
	return requests, accepts
}
