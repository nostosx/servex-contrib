package viper

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	upstream "github.com/spf13/viper"

	"github.com/nostosx/servex/config"
)

// Validator checks a fully merged candidate before it is published. It may
// retain the snapshot or read the Config, but must not call Reload on the same
// Config: validation runs while reloads are serialized.
type Validator func(context.Context, config.Reader) error

// Option configures a Config constructed by New.
type Option func(*options)

type options struct {
	validators []Validator
}

// WithValidator appends v to the validators, which run in registration order
// on initial load and every reload. A nil validator is ignored. The first
// error rejects the candidate and is wrapped by Reload.
func WithValidator(v Validator) Option {
	return func(o *options) {
		if v != nil {
			o.validators = append(o.validators, v)
		}
	}
}

// Config holds the merged configuration of its sources and can be reloaded.
// All methods are safe for concurrent use. Use Snapshot to pin one revision
// across multiple reads.
type Config struct {
	sources    []config.Source
	validators []Validator
	reload     sync.Mutex // serializes load, merge, validation and publication
	data       atomic.Pointer[Snapshot]
}

// New loads every source in order and deep-merges the results: later sources
// override earlier ones, maps are merged recursively and every other value
// is replaced as a whole. opts apply before the initial load; validators see
// the final merged tree, so later sources may repair values from earlier
// sources. New returns nil if loading or validation fails or ctx is canceled
// before publication. Nil options are ignored.
func New(ctx context.Context, sources []config.Source, opts ...Option) (*Config, error) {
	for i, s := range sources {
		if s == nil {
			return nil, fmt.Errorf("config: source %d is nil", i)
		}
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	c := &Config{sources: slices.Clone(sources), validators: o.validators}
	if err := c.Reload(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Reload serializes loading, merging, validation and atomic publication of
// the new values. Any error or cancellation before publication keeps the
// previous values. Validators must not call Reload on this Config.
func (c *Config) Reload(ctx context.Context) error {
	c.reload.Lock()
	defer c.reload.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("config: reload: %w", err)
	}
	candidate := upstream.New()
	for i, s := range c.sources {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("config: load source %d (%T): %w", i, s, err)
		}
		m, err := s.Load(ctx)
		if err != nil {
			return fmt.Errorf("config: load source %d (%T): %w", i, s, err)
		}
		apply(candidate, "", normalize(m).(map[string]any))
	}
	for i, validate := range c.validators {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("config: validate %d: %w", i, err)
		}
		if err := validate(ctx, &Snapshot{data: normalize(candidate.AllSettings()).(map[string]any)}); err != nil {
			return fmt.Errorf("config: validate %d: %w", i, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("config: reload: %w", err)
	}
	c.data.Store(&Snapshot{data: normalize(candidate.AllSettings()).(map[string]any)})
	return nil
}

// Snapshot pins the currently published configuration. Further reloads do
// not change the returned snapshot.
func (c *Config) Snapshot() config.Reader {
	if s := c.data.Load(); s != nil {
		return &Snapshot{data: s.data}
	}
	return &Snapshot{}
}

// Value returns a deep copy of the value at the dotted path, such as
// "broker.kafka.brokers". The empty path returns the whole tree. Numeric
// segments index into lists.
func (c *Config) Value(path string) (any, bool) {
	return c.Snapshot().Value(path)
}

// String returns the value at path as a string. Numbers and booleans are
// formatted; a missing or non-scalar value yields "".
func (c *Config) String(path string) string {
	return c.Snapshot().String(path)
}

// Int returns the value at path as an int. Integral numbers and decimal
// strings are converted; a missing or unconvertible value yields 0.
func (c *Config) Int(path string) int {
	return c.Snapshot().Int(path)
}

// Bool returns the value at path as a bool. Strings accepted by
// strconv.ParseBool are converted; a missing or unconvertible value yields
// false.
func (c *Config) Bool(path string) bool {
	return c.Snapshot().Bool(path)
}

// Float64 returns the value at path as a float64. Numeric strings are
// converted; a missing or unconvertible value yields 0.
func (c *Config) Float64(path string) float64 {
	return c.Snapshot().Float64(path)
}

// Duration returns the value at path as a time.Duration. It accepts duration
// strings such as "5s" and numbers, which are interpreted as nanoseconds; a
// missing or unconvertible value yields 0.
func (c *Config) Duration(path string) time.Duration {
	return c.Snapshot().Duration(path)
}

// Unmarshal decodes the subtree at prefix ("" for the whole tree) into dst,
// which must be a non-nil pointer, typically to a struct or a map. A missing
// prefix leaves dst unchanged.
//
// Struct fields are matched by their json tag name, or by the field name when
// untagged; an exact key is preferred and a case-insensitive match is used
// as a fallback. A json tag of "-" skips the field, and untagged embedded
// structs have their fields promoted. Values are converted weakly: strings
// to numbers, booleans and time.Duration; numbers to time.Duration
// (nanoseconds); scalars and objects to strings where meaningful; a single
// value to a one-element slice. Types implementing encoding.TextUnmarshaler
// are decoded from strings. Errors name the offending path.
func (c *Config) Unmarshal(prefix string, dst any) error {
	return c.Snapshot().Unmarshal(prefix, dst)
}

// WithReloadErrorHandler reports watch-triggered reload failures. It runs
// synchronously outside the reload lock and may call Reload.
func WithReloadErrorHandler(fn func(error)) config.WatchOption {
	return func(o *config.WatchOptions) { o.OnReloadError = fn }
}

// Watch runs Watch on every Watchable source. After a change it reloads the
// configuration and then calls fn with c; a failed reload keeps the previous
// values, skips fn until the next change and is reported to the handler set
// by WithReloadErrorHandler, if any. Bursts of changes are coalesced and
// callbacks do not overlap within one Watch call. Both fn and the error
// handler run synchronously outside the reload lock; they may call Reload.
// fn receives c itself, so manual concurrent reloads may advance its values
// before fn reads them. Pin a Snapshot for consistent reads within a
// callback. Nil options are ignored.
//
// Watch blocks until ctx is done and then returns nil; if a source stops
// watching with an error before that, Watch stops the others and returns the
// error. It waits for all source watchers to return, so they must honor
// cancellation.
func (c *Config) Watch(ctx context.Context, fn func(config.Config), opts ...config.WatchOption) error {
	var o config.WatchOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	changed := make(chan struct{}, 1)
	notify := func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	}
	errc := make(chan error, len(c.sources))
	for i, s := range c.sources {
		w, ok := s.(config.Watchable)
		if !ok {
			continue
		}
		wg.Go(func() {
			if err := w.Watch(ctx, notify); err != nil && ctx.Err() == nil {
				errc <- fmt.Errorf("config: watch source %d (%T): %w", i, s, err)
			}
		})
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			return err
		case <-changed:
			if err := c.Reload(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if o.OnReloadError != nil {
					o.OnReloadError(err)
				}
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			if fn != nil {
				fn(c)
			}
		}
	}
}

var _ config.Config = (*Config)(nil)

// apply delegates storage and case folding to Viper.Set. Walking object leaves
// allows ordered Sources to replace an object with a scalar and vice versa;
// MergeConfigMap alone silently ignores the object-to-scalar transition.
func apply(v *upstream.Viper, prefix string, tree map[string]any) {
	for _, key := range slices.Sorted(maps.Keys(tree)) {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if child, ok := tree[key].(map[string]any); ok {
			if _, exists := v.Get(path).(map[string]any); !exists {
				v.Set(path, map[string]any{})
			}
			apply(v, path, child)
		} else {
			v.Set(path, tree[key])
		}
	}
}
