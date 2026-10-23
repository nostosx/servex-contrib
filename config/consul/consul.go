package consul

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/errorx"
)

// Error reasons used by Source.
const (
	// ReasonUnavailable marks a failure to reach Consul or an error
	// response from it.
	ReasonUnavailable = "CONFIG_SOURCE_UNAVAILABLE"
	// ReasonNotFound marks a missing key in single key mode.
	ReasonNotFound = "CONFIG_NOT_FOUND"
	// ReasonDecode marks a value that the codec cannot decode.
	ReasonDecode = "CONFIG_DECODE_FAILED"
)

// Defaults for Watch.
const (
	minRetryDelay = 100 * time.Millisecond
	maxRetryDelay = 10 * time.Second
)

// Compile-time checks that Source satisfies the config contracts.
var (
	_ config.Source    = (*Source)(nil)
	_ config.Watchable = (*Source)(nil)
)

// Source reads configuration from the Consul KV store. It implements
// config.Source and config.Watchable and is safe for concurrent use. The
// client is owned by the caller; datacenter, namespace, partition and token
// come from its configuration.
type Source struct {
	kv       *api.KV
	key      string
	prefix   bool
	codec    encoding.Codec
	wait     time.Duration
	minDelay time.Duration
	maxDelay time.Duration
	// last is the snapshot observed by the latest Load, so Watch can block
	// on its index without missing changes in between.
	last atomic.Pointer[snapshot]
}

// snapshot identifies one observed state of the watched keys.
type snapshot struct {
	index uint64
	// keys lists every matching key with its modify index, in key order.
	keys []version
}

// version is the modify index of one key.
type version struct {
	key   string
	index uint64
}

// Option configures a Source.
type Option func(*options)

// options holds the settings collected from Options.
type options struct {
	prefix   bool
	codec    encoding.Codec
	wait     time.Duration
	minDelay time.Duration
	maxDelay time.Duration
}

// New returns a Source reading key through client. Nothing is read until
// Load is called.
func New(client *api.Client, key string, opts ...Option) *Source {
	o := options{codec: encoding.JSON, minDelay: minRetryDelay, maxDelay: maxRetryDelay}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Source{
		kv:       client.KV(),
		key:      key,
		prefix:   o.prefix,
		codec:    o.codec,
		wait:     o.wait,
		minDelay: o.minDelay,
		maxDelay: max(o.minDelay, o.maxDelay),
	}
}

// WithPrefix treats key as a prefix: every key under it is loaded and the
// results are merged into one tree. The rest of each key after the prefix,
// split at "/" with empty segments ignored, is the path of its decoded
// value in the tree, so with prefix "app/" the key "app/db/primary" holding
// {"dsn":"x"} yields {"db":{"primary":{"dsn":"x"}}}. The key equal to the
// prefix itself must hold an object, which is merged at the root, and
// folder keys without a value yield empty objects. Keys are merged in
// ascending order: objects are merged recursively and any other value
// replaces what was there, so a deeper key overrides the matching field of
// a shallower one. End the prefix with "/" to avoid matching sibling keys
// such as "app2".
func WithPrefix() Option {
	return func(o *options) { o.prefix = true }
}

// WithCodec sets the codec used to decode values. In single key mode the
// value must decode to an object. With WithPrefix, values below the prefix may
// decode to any type, but the prefix key itself must still hold an object.
// A nil codec keeps the default, encoding.JSON.
func WithCodec(c encoding.Codec) Option {
	return func(o *options) {
		if c != nil {
			o.codec = c
		}
	}
}

// WithWaitTime sets the maximum duration of one blocking query. Consul caps
// it at 10 minutes. Non-positive values keep the server default of 5
// minutes.
func WithWaitTime(d time.Duration) Option {
	return func(o *options) { o.wait = max(d, 0) }
}

// WithRetryDelay sets the bounds of the exponential delay between Watch
// retries after transient failures. Non-positive values keep the defaults
// of 100ms and 10s.
func WithRetryDelay(minDelay, maxDelay time.Duration) Option {
	return func(o *options) {
		if minDelay > 0 {
			o.minDelay = minDelay
		}
		if maxDelay > 0 {
			o.maxDelay = maxDelay
		}
	}
}

// Load reads the key, or every key under the prefix, and decodes it. In
// single key mode a missing key is an errorx.NotFound error, and a value
// containing only white space yields an empty map; with WithPrefix no
// matching keys yield an empty map. Failures to reach Consul are retryable
// errorx.Unavailable errors, error responses are classified by status code
// and undecodable values are errorx.InvalidArgument errors.
func (s *Source) Load(ctx context.Context) (map[string]any, error) {
	pairs, meta, err := s.fetch(ctx, 0)
	if err != nil {
		return nil, s.classify("load", err)
	}
	s.last.Store(newSnapshot(meta.LastIndex, pairs))
	if !s.prefix {
		if len(pairs) == 0 {
			return nil, errorx.Newf(errorx.NotFound, ReasonNotFound, "consul: load %q: key not found", s.key)
		}
		m := map[string]any{}
		if err := s.decode(pairs[0], &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	root := map[string]any{}
	for _, p := range pairs {
		var v any
		if err := s.decode(p, &v); err != nil {
			return nil, err
		}
		path := segments(strings.TrimPrefix(p.Key, s.key))
		if err := insert(root, path, v); err != nil {
			return nil, errorx.Newf(errorx.InvalidArgument, ReasonDecode, "consul: load %q", p.Key).WithCause(err)
		}
	}
	return root, nil
}

// fetch reads the matching pairs, sorted by key. A positive index makes it
// a blocking query.
func (s *Source) fetch(ctx context.Context, index uint64) (api.KVPairs, *api.QueryMeta, error) {
	q := (&api.QueryOptions{WaitIndex: index, WaitTime: s.wait}).WithContext(ctx)
	if !s.prefix {
		pair, meta, err := s.kv.Get(s.key, q)
		if err != nil || pair == nil {
			return nil, meta, err
		}
		return api.KVPairs{pair}, meta, nil
	}
	pairs, meta, err := s.kv.List(s.key, q)
	if err != nil {
		return nil, nil, err
	}
	slices.SortFunc(pairs, func(a, b *api.KVPair) int { return strings.Compare(a.Key, b.Key) })
	return pairs, meta, nil
}

// decode decodes the value of p with the codec into dst, a *map[string]any
// or *any. A value containing only white space yields an empty map.
func (s *Source) decode(p *api.KVPair, dst any) error {
	if len(bytes.TrimSpace(p.Value)) == 0 {
		switch d := dst.(type) {
		case *map[string]any:
			*d = map[string]any{}
		case *any:
			*d = map[string]any{}
		}
		return nil
	}
	if err := s.codec.Unmarshal(p.Value, dst); err != nil {
		return errorx.Newf(errorx.InvalidArgument, ReasonDecode,
			"consul: decode %q with %s codec", p.Key, s.codec.Name()).WithCause(err)
	}
	return nil
}

// Watch runs blocking queries on the key, or the prefix, and calls notify
// once for every observed change: a key being created, written (even with
// the same value) or deleted. Several changes between two queries are
// reported once. Watch continues from the state read by the latest Load,
// so no change made after a Load is missed; without a prior Load it starts
// from the current state. Transient failures are retried with exponential
// delay. Watch returns nil once ctx is done and an error when Consul
// rejects the query with a non-retryable status, such as 403.
func (s *Source) Watch(ctx context.Context, notify func()) error {
	prev := s.last.Load()
	delay := s.minDelay
	for {
		index := uint64(0)
		if prev != nil {
			index = prev.index
		}
		pairs, meta, err := s.fetch(ctx, index)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			err = s.classify("watch", err)
			if !errorx.IsRetryable(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
			delay = min(2*delay, s.maxDelay)
			continue
		}
		delay = s.minDelay
		cur := newSnapshot(meta.LastIndex, pairs)
		if prev != nil && !slices.Equal(cur.keys, prev.keys) {
			notify()
		}
		switch {
		case cur.index == 0:
			// Never block on index 0, which would return at once forever.
			cur.index = 1
		case prev != nil && cur.index < prev.index:
			// The index went backwards, for example after a snapshot
			// restore: start over with a non-blocking query.
			cur.index = 0
		}
		prev = cur
	}
}

// newSnapshot records the index and key versions of a query result.
func newSnapshot(index uint64, pairs api.KVPairs) *snapshot {
	keys := make([]version, len(pairs))
	for i, p := range pairs {
		keys[i] = version{key: p.Key, index: p.ModifyIndex}
	}
	return &snapshot{index: index, keys: keys}
}

// classify maps a failed Consul call to an errorx error: context errors
// keep their own code, HTTP status errors are mapped by status and other
// failures are retryable errorx.Unavailable errors.
func (s *Source) classify(op string, err error) error {
	code := errorx.Unavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = errorx.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = errorx.DeadlineExceeded
	default:
		if se, ok := errors.AsType[api.StatusError](err); ok {
			code = statusCode(se.Code)
		}
	}
	return errorx.Newf(code, ReasonUnavailable, "consul: %s %q", op, s.key).WithCause(err)
}

// statusCode maps an HTTP status returned by Consul to an errorx code.
func statusCode(status int) errorx.Code {
	switch status {
	case http.StatusBadRequest:
		return errorx.InvalidArgument
	case http.StatusUnauthorized:
		return errorx.Unauthenticated
	case http.StatusForbidden:
		return errorx.PermissionDenied
	case http.StatusTooManyRequests:
		return errorx.ResourceExhausted
	}
	return errorx.Unavailable
}
