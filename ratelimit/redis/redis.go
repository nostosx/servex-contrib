// Package redis provides an atomic Redis token bucket shared by service
// replicas. It implements the structural ContextLimiter contract consumed by
// servex-contrib/middleware/ratelimit without importing another contrib module.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/errorx"
)

// Limiter holds a policy; the caller owns the Redis client. Replicas using
// the same prefix must use identical rate and burst settings. Redis server
// time drives refill; idle keys expire once their bucket would be full.
type Limiter struct {
	client redis.UniversalClient
	rate   float64
	burst  int
	ttlMS  int64
	prefix string
}

// Option configures a Redis limiter.
type Option func(*Limiter)

// WithPrefix selects the policy namespace. The default is "servex:ratelimit:".
// Use a distinct prefix for each independent policy or trust domain.
func WithPrefix(prefix string) Option { return func(l *Limiter) { l.prefix = prefix } }

// New creates a bucket granting rate tokens per second with burst capacity.
// It does not connect to Redis. Policy values must be finite and positive,
// and the full refill time must fit a time.Duration. The client should have
// ContextTimeoutEnabled enabled so caller deadlines bound socket operations.
func New(client redis.UniversalClient, rate float64, burst int, opts ...Option) (*Limiter, error) {
	const maxMS = math.MaxInt64 / int64(time.Millisecond)
	ttl := math.Ceil(float64(burst) / rate * 1000)
	if client == nil || rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst <= 0 || uint64(burst) > 1<<53 || math.IsNaN(ttl) || math.IsInf(ttl, 0) || ttl > float64(maxMS) {
		return nil, errorx.New(errorx.InvalidArgument, "REDIS_RATE_LIMIT_CONFIG", "redis ratelimit: invalid client, rate or burst")
	}
	l := &Limiter{client: client, rate: rate, burst: burst, ttlMS: max(1, int64(ttl)), prefix: "servex:ratelimit:"}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}
	if l.prefix == "" {
		return nil, errorx.New(errorx.InvalidArgument, "REDIS_RATE_LIMIT_CONFIG", "redis ratelimit: empty prefix")
	}
	return l, nil
}

var bucketScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rate, burst, ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens, last = burst, now
if state[1] then
  tokens, last = tonumber(state[1]), tonumber(state[2])
  if not tokens or not last then return redis.error_reply('invalid rate limit state') end
end
now = math.max(now, last)
tokens = math.min(burst, tokens + (now - last) * rate / 1000)
local allowed, retry = 0, 0
if tokens >= 1 then
  allowed, tokens = 1, tokens - 1
else
  retry = math.max(1, math.ceil((1 - tokens) * 1000 / rate))
end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens), 'last', string.format('%.0f', now))
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, retry}
`)

// AllowContext spends one token if available. Rejection returns a positive
// retry hint; backend failures are errors. Cancellation preserves context
// identity. It does not retry in application code; the injected client's
// retry policy remains in effect.
func (l *Limiter) AllowContext(ctx context.Context, key string) (bool, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	hash := sha256.Sum256([]byte(key))
	redisKey := l.prefix + hex.EncodeToString(hash[:])
	result, err := bucketScript.Run(ctx, l.client, []string{redisKey}, l.rate, l.burst, l.ttlMS).Result()
	if ctx.Err() != nil {
		return false, 0, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, redis.ErrClosed) {
			return false, 0, errorx.New(errorx.FailedPrecondition, "REDIS_CLIENT_CLOSED", "redis ratelimit: client closed").WithCause(err)
		}
		return false, 0, errorx.New(errorx.Unavailable, "REDIS_RATE_LIMIT_BACKEND", "redis ratelimit: backend unavailable").WithCause(err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return false, 0, errorx.New(errorx.Internal, "REDIS_RATE_LIMIT_RESPONSE", "redis ratelimit: invalid script response")
	}
	allowed, ok1 := values[0].(int64)
	ms, ok2 := values[1].(int64)
	if !ok1 || !ok2 || (allowed != 0 && allowed != 1) || ms < 0 || ms > math.MaxInt64/int64(time.Millisecond) {
		return false, 0, errorx.New(errorx.Internal, "REDIS_RATE_LIMIT_RESPONSE", "redis ratelimit: invalid script values")
	}
	return allowed == 1, time.Duration(ms) * time.Millisecond, nil
}
