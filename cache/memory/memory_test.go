package memory

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	cacheloader "github.com/nostosx/servex-contrib/cache/loader"
	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	"github.com/nostosx/servex/storage/cache"
)

// fakeClock is a manually advanced clock for WithClock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

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

func mustSet(t *testing.T, c *Cache, key, value string, ttl time.Duration) {
	t.Helper()
	if err := c.Set(t.Context(), key, []byte(value), ttl); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

// lookup returns the value of key and whether it was found, failing on any
// error other than cache.ErrNotFound.
func lookup(t *testing.T, c *Cache, key string) (string, bool) {
	t.Helper()
	v, err := c.Get(t.Context(), key)
	switch {
	case errors.Is(err, cache.ErrNotFound):
		return "", false
	case err != nil:
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v), true
}

func TestGetSetDelete(t *testing.T) {
	c := New()
	if _, err := c.Get(t.Context(), "missing"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
	mustSet(t, c, "k", "v1", 0)
	if v, ok := lookup(t, c, "k"); !ok || v != "v1" {
		t.Fatalf("Get = %q, %v; want v1", v, ok)
	}
	mustSet(t, c, "k", "v2", 0)
	if v, _ := lookup(t, c, "k"); v != "v2" {
		t.Fatalf("after overwrite Get = %q, want v2", v)
	}
	for range 2 { // Delete is idempotent.
		if err := c.Delete(t.Context(), "k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if _, ok := lookup(t, c, "k"); ok {
		t.Fatal("entry survived Delete")
	}
	if n := c.items.Len(); n != 0 {
		t.Fatalf("size = %d, want 0", n)
	}
}

func TestTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		advance time.Duration
		wantHit bool
	}{
		{name: "before expiry", ttl: time.Minute, advance: time.Minute - time.Nanosecond, wantHit: true},
		{name: "at expiry", ttl: time.Minute, advance: time.Minute},
		{name: "after expiry", ttl: time.Minute, advance: time.Hour},
		{name: "zero ttl never expires", ttl: 0, advance: 1000 * time.Hour, wantHit: true},
		{name: "negative ttl never expires", ttl: -time.Second, advance: 1000 * time.Hour, wantHit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeClock()
			c := New(WithClock(clock.Now))
			mustSet(t, c, "k", "v", tt.ttl)
			clock.Advance(tt.advance)
			if _, ok := lookup(t, c, "k"); ok != tt.wantHit {
				t.Fatalf("hit = %v, want %v", ok, tt.wantHit)
			}
			// Expired entries are removed lazily by the access above.
			if want := map[bool]int{true: 1, false: 0}[tt.wantHit]; c.items.Len() != want {
				t.Fatalf("size = %d, want %d", c.items.Len(), want)
			}
		})
	}
}

func TestOverwriteResetsTTL(t *testing.T) {
	clock := newFakeClock()
	c := New(WithClock(clock.Now))

	mustSet(t, c, "a", "1", time.Second)
	mustSet(t, c, "a", "2", 0) // clears the expiry
	mustSet(t, c, "b", "1", 0)
	mustSet(t, c, "b", "2", time.Second) // adds an expiry
	mustSet(t, c, "c", "1", time.Second)
	mustSet(t, c, "c", "2", time.Hour) // extends the expiry
	clock.Advance(time.Minute)

	for _, tc := range []struct {
		key     string
		wantHit bool
	}{{"a", true}, {"b", false}, {"c", true}} {
		key, wantHit := tc.key, tc.wantHit
		if _, ok := lookup(t, c, key); ok != wantHit {
			t.Errorf("%s: hit = %v, want %v", key, ok, wantHit)
		}
	}
}

func TestDefaultClockWithSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		mustSet(t, c, "k", "v", time.Second)
		synctest.Sleep(999 * time.Millisecond)
		if _, ok := lookup(t, c, "k"); !ok {
			t.Fatal("entry expired early")
		}
		synctest.Sleep(time.Millisecond)
		if _, ok := lookup(t, c, "k"); ok {
			t.Fatal("entry did not expire")
		}
	})
}

func TestLRUEviction(t *testing.T) {
	c := New(WithMaxEntries(2))
	mustSet(t, c, "a", "1", 0)
	mustSet(t, c, "b", "2", 0)
	lookup(t, c, "a") // a becomes most recently used
	mustSet(t, c, "c", "3", 0)
	if _, ok := lookup(t, c, "b"); ok {
		t.Fatal("least recently used entry survived eviction")
	}

	// Overwriting an entry also marks it as most recently used.
	mustSet(t, c, "a", "1'", 0)
	mustSet(t, c, "d", "4", 0)
	if _, ok := lookup(t, c, "c"); ok {
		t.Fatal("overwrite did not refresh recency")
	}
	if v, ok := lookup(t, c, "a"); !ok || v != "1'" {
		t.Errorf("a = %q, %v", v, ok)
	}
}

func TestEvictionPrefersExpired(t *testing.T) {
	clock := newFakeClock()
	c := New(WithMaxEntries(3), WithClock(clock.Now))
	mustSet(t, c, "lru", "1", 0)
	mustSet(t, c, "short", "2", time.Second)
	mustSet(t, c, "long", "3", time.Hour)
	clock.Advance(time.Minute) // "short" expires; "lru" is still the LRU entry

	mustSet(t, c, "new", "4", 0)
	if _, ok := lookup(t, c, "short"); ok {
		t.Fatal("expired entry survived insertion")
	}

	// Without expired entries, eviction falls back to LRU order.
	mustSet(t, c, "newer", "5", 0)
	if _, ok := lookup(t, c, "lru"); ok {
		t.Fatal("least recently used entry survived insertion")
	}
	for _, key := range []string{"long", "new", "newer"} {
		if _, ok := lookup(t, c, key); !ok {
			t.Errorf("live entry %q was evicted", key)
		}
	}
}

func TestMaxEntries(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		set  int
		want int
	}{
		{name: "default", set: defaultMaxEntries + 5, want: defaultMaxEntries},
		{name: "zero is unlimited", opts: []Option{WithMaxEntries(0)}, set: defaultMaxEntries + 5, want: defaultMaxEntries + 5},
		{name: "negative is unlimited", opts: []Option{WithMaxEntries(-1)}, set: 50, want: 50},
		{name: "explicit", opts: []Option{WithMaxEntries(10)}, set: 50, want: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(tt.opts...)
			for i := range tt.set {
				mustSet(t, c, strconv.Itoa(i), "v", 0)
			}
			if n := c.items.Len(); n != tt.want {
				t.Fatalf("size = %d, want %d", n, tt.want)
			}
			// The newest entry always survives.
			if _, ok := lookup(t, c, strconv.Itoa(tt.set-1)); !ok {
				t.Fatal("newest entry was evicted")
			}
		})
	}
}

func TestValuesAreCopied(t *testing.T) {
	c := New()
	in := []byte("hello")
	if err := c.Set(t.Context(), "k", in, 0); err != nil {
		t.Fatal(err)
	}
	in[0] = 'J' // mutating the caller's slice must not affect the cache

	out, err := c.Get(t.Context(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello" {
		t.Fatalf("Get = %q after mutating input, want hello", out)
	}
	out[0] = 'Y' // mutating the returned slice must not affect the cache
	_ = append(out[:1], "XXXX"...)

	if v, _ := lookup(t, c, "k"); v != "hello" {
		t.Fatalf("Get = %q after mutating output, want hello", v)
	}
}

func TestEmptyValues(t *testing.T) {
	c := New()
	for key, value := range map[string][]byte{"nil": nil, "empty": {}} {
		if err := c.Set(t.Context(), key, value, 0); err != nil {
			t.Fatal(err)
		}
		got, err := c.Get(t.Context(), key)
		if err != nil || len(got) != 0 {
			t.Fatalf("Get(%q) = %q, %v; want empty hit", key, got, err)
		}
	}
}

func TestLoaderIntegration(t *testing.T) {
	c := New()
	l, err := cacheloader.New[map[string]int](t.Context(), c, time.Minute, cacheloader.WithCodec(jsoncodec.Codec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var calls atomic.Int32
	load := func(context.Context) (map[string]int, error) {
		calls.Add(1)
		return map[string]int{"n": 1}, nil
	}
	for range 3 {
		v, err := l.Get(t.Context(), "k", load)
		if err != nil || v["n"] != 1 {
			t.Fatalf("Get = %v, %v", v, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("load called %d times, want 1", n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	clock := newFakeClock()
	c := New(WithMaxEntries(64), WithClock(clock.Now))
	ctx := t.Context()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 500 {
				key := fmt.Sprintf("k%d", (g*31+i)%100)
				switch i % 4 {
				case 0:
					_ = c.Set(ctx, key, []byte(key), time.Duration(i%3)*time.Second)
				case 1:
					if v, err := c.Get(ctx, key); err == nil && string(v) != key {
						t.Errorf("Get(%q) = %q", key, v)
					}
				case 2:
					_ = c.Delete(ctx, key)
				case 3:
					clock.Advance(time.Second)
				}
			}
		})
	}
	wg.Wait()
	if n := c.items.Len(); n > 64 {
		t.Fatalf("size = %d exceeds max entries", n)
	}
}

func BenchmarkSetGet(b *testing.B) {
	c := New()
	ctx := b.Context()
	value := []byte("value")
	i := 0
	for b.Loop() {
		key := strconv.Itoa(i % 1024)
		_ = c.Set(ctx, key, value, time.Minute)
		_, _ = c.Get(ctx, key)
		i++
	}
}
