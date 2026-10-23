package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/errorx"
)

// Error reasons used by Source.
const (
	// ReasonUnavailable marks a failure to reach etcd.
	ReasonUnavailable = "CONFIG_SOURCE_UNAVAILABLE"
	// ReasonNotFound marks a missing key in single key mode.
	ReasonNotFound = "CONFIG_NOT_FOUND"
	// ReasonDecode marks a value that the codec cannot decode.
	ReasonDecode = "CONFIG_DECODE_FAILED"
)

// Compile-time checks that Source satisfies the config contracts.
var (
	_ config.Source    = (*Source)(nil)
	_ config.Watchable = (*Source)(nil)
)

// Source reads configuration from etcd. It implements config.Source and
// config.Watchable and is safe for concurrent use. The client is owned by
// the caller and is never closed by Source.
type Source struct {
	client *clientv3.Client
	key    string
	prefix bool
	codec  encoding.Codec
	// rev is the store revision observed by the latest Load, so Watch can
	// resume right after it without missing changes in between.
	rev atomic.Int64
}

// Option configures a Source.
type Option func(*options)

// options holds the settings collected from Options.
type options struct {
	prefix bool
	codec  encoding.Codec
}

// New returns a Source reading key through client. Nothing is read until
// Load is called.
func New(client *clientv3.Client, key string, opts ...Option) *Source {
	o := options{codec: encoding.JSON}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Source{client: client, key: key, prefix: o.prefix, codec: o.codec}
}

// WithPrefix treats key as a prefix: every key under it is loaded and the
// results are merged into one tree. The rest of each key after the prefix,
// split at "/" with empty segments ignored, is the path of its decoded
// value in the tree, so with prefix "/app/" the key "/app/db/primary"
// holding {"dsn":"x"} yields {"db":{"primary":{"dsn":"x"}}}. The key equal
// to the prefix itself must hold an object, which is merged at the root.
// Keys are merged in ascending order: objects are merged recursively and
// any other value replaces what was there, so a deeper key overrides the
// matching field of a shallower one. End the prefix with "/" to avoid
// matching sibling keys such as "/app2".
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

// Load reads the key, or every key under the prefix, and decodes it. In
// single key mode a missing key is an errorx.NotFound error, and a value
// containing only white space yields an empty map; with WithPrefix no
// matching keys yield an empty map. Failures to reach etcd are retryable
// errorx.Unavailable errors and undecodable values errorx.InvalidArgument
// errors.
func (s *Source) Load(ctx context.Context) (map[string]any, error) {
	var opts []clientv3.OpOption
	if s.prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	resp, err := s.client.Get(ctx, s.key, opts...)
	if err != nil {
		return nil, unavailable(fmt.Sprintf("etcd: load %q", s.key), err)
	}
	s.rev.Store(resp.Header.GetRevision())
	if !s.prefix {
		if len(resp.Kvs) == 0 {
			return nil, errorx.Newf(errorx.NotFound, ReasonNotFound, "etcd: load %q: key not found", s.key)
		}
		m := map[string]any{}
		if err := s.decode(resp.Kvs[0].Key, resp.Kvs[0].Value, &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	root := map[string]any{}
	for _, kv := range resp.Kvs {
		var v any
		if err := s.decode(kv.Key, kv.Value, &v); err != nil {
			return nil, err
		}
		path := segments(strings.TrimPrefix(string(kv.Key), s.key))
		if err := insert(root, path, v); err != nil {
			return nil, errorx.Newf(errorx.InvalidArgument, ReasonDecode, "etcd: load %q", kv.Key).WithCause(err)
		}
	}
	return root, nil
}

// decode decodes value with the codec into dst, a *map[string]any or *any.
// A value containing only white space yields an empty map.
func (s *Source) decode(key, value []byte, dst any) error {
	if len(bytes.TrimSpace(value)) == 0 {
		switch d := dst.(type) {
		case *map[string]any:
			*d = map[string]any{}
		case *any:
			*d = map[string]any{}
		}
		return nil
	}
	if err := s.codec.Unmarshal(value, dst); err != nil {
		return errorx.Newf(errorx.InvalidArgument, ReasonDecode,
			"etcd: decode %q with %s codec", key, s.codec.Name()).WithCause(err)
	}
	return nil
}

// Watch watches the key, or the prefix, and calls notify once for every
// watch response carrying changes; all events of one transaction arrive in
// one response. It resumes right after the revision read by the latest
// Load, or starts at the current revision when Load has not been called, so
// no change made after a Load is missed. When etcd has compacted the
// revisions Watch would resume from, it calls notify once and continues
// from the current revision. Watch returns nil once ctx is done and a
// retryable errorx.Unavailable error when etcd ends the watch, for example
// because the client was closed.
func (s *Source) Watch(ctx context.Context, notify func()) error {
	rev := s.rev.Load()
	for {
		compacted, err := s.watch(ctx, rev, notify)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if compacted {
			// The resume revision was compacted: the data may have changed
			// in the gap, so report one change and start over from now.
			notify()
			rev = 0
		}
	}
}

// watch runs one etcd watch, starting after revision rev or at the current
// revision when rev is 0, until it ends. It reports compacted=true when the
// watch was canceled because its start revision has been compacted.
func (s *Source) watch(ctx context.Context, rev int64, notify func()) (compacted bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts := []clientv3.OpOption{}
	if s.prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev+1))
	}
	for resp := range s.client.Watch(ctx, s.key, opts...) {
		if resp.CompactRevision != 0 {
			return true, nil
		}
		if err := resp.Err(); err != nil {
			return false, unavailable(fmt.Sprintf("etcd: watch %q", s.key), err)
		}
		if len(resp.Events) > 0 {
			notify()
		}
	}
	return false, errorx.Newf(errorx.Unavailable, ReasonUnavailable, "etcd: watch %q: watch channel closed", s.key)
}

// unavailable classifies a failed etcd call: context errors keep their own
// code and everything else is a retryable errorx.Unavailable error.
func unavailable(msg string, err error) error {
	code := errorx.Unavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = errorx.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = errorx.DeadlineExceeded
	}
	return errorx.New(code, ReasonUnavailable, msg).WithCause(err)
}
