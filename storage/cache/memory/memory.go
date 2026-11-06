package memory

import (
	"context"
	"slices"
	"time"

	"github.com/nostosx/kitx/ttlcache"

	"github.com/nostosx/servex/storage/cache"
)

// defaultMaxEntries bounds the cache when WithMaxEntries is not given.
const defaultMaxEntries = 10000

// Option configures a Cache.
type Option func(*options)

type options struct {
	maxEntries int
	now        func() time.Time
}

// WithMaxEntries sets the maximum number of entries. When a Set would exceed
// it, an expired entry is evicted if there is one, otherwise the least
// recently used entry. The default is 10000; n <= 0 means unlimited.
func WithMaxEntries(n int) Option {
	return func(o *options) { o.maxEntries = n }
}

// WithClock sets the clock used for expiry, mainly for tests. The default is
// time.Now; nil keeps the default. It runs under the cache lock and must not
// reenter the cache.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// Cache is an in-memory cache.Cache. Stored and returned values are copies,
// so callers may modify input slices after Set returns and returned slices
// independently. Methods are safe for concurrent use and ignore their contexts;
// Get returns cache.ErrNotFound on a miss and other operations always succeed.
// Create a Cache with New; its zero value is not usable.
type Cache struct {
	items *ttlcache.Cache[string, []byte]
}

var _ cache.Cache = (*Cache)(nil)

// New returns an empty Cache.
func New(opts ...Option) *Cache {
	o := options{maxEntries: defaultMaxEntries, now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.maxEntries <= 0 {
		return &Cache{items: ttlcache.NewUnbounded[string, []byte](ttlcache.WithClock(o.now))}
	}
	items, err := ttlcache.New[string, []byte](o.maxEntries, ttlcache.WithClock(o.now))
	if err != nil {
		panic(err) // The capacity above is strictly positive.
	}
	return &Cache{items: items}
}

// Get returns a copy of the value stored under key, or cache.ErrNotFound if
// it is absent or expired. A hit marks the entry as most recently used; an
// expired entry is removed.
func (c *Cache) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := c.items.Get(key)
	if !ok {
		return nil, cache.ErrNotFound
	}
	return slices.Clone(value), nil
}

// Set stores a copy of value under key, replacing any existing entry. A
// ttl <= 0 means the entry never expires.
func (c *Cache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	c.items.Set(key, slices.Clone(value), ttl)
	return nil
}

// Delete removes key. Deleting an absent key is not an error.
func (c *Cache) Delete(_ context.Context, key string) error {
	c.items.Delete(key)
	return nil
}
