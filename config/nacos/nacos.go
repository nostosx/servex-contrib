package nacos

import (
	"context"
	"errors"
	"strings"
	"sync"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/errorx"
)

// DefaultGroup is the group used when New is given an empty group, as in
// the Nacos client.
const DefaultGroup = "DEFAULT_GROUP"

// Error reasons used by Source.
const (
	// ReasonUnavailable marks a failure of the Nacos client.
	ReasonUnavailable = "CONFIG_SOURCE_UNAVAILABLE"
	// ReasonInvalid marks an unusable data ID.
	ReasonInvalid = "CONFIG_INVALID_SOURCE"
	// ReasonDecode marks content that the codec cannot decode.
	ReasonDecode = "CONFIG_DECODE_FAILED"
)

// Compile-time checks that Source satisfies the config contracts.
var (
	_ config.Source    = (*Source)(nil)
	_ config.Watchable = (*Source)(nil)
)

// Source reads one configuration item from Nacos. It implements
// config.Source and config.Watchable and is safe for concurrent use. The
// client is owned by the caller; the namespace comes from its
// configuration.
type Source struct {
	client config_client.IConfigClient
	dataID string
	group  string
	codec  encoding.Codec

	// mu guards watchers. Concurrent Watch calls share one Nacos listener,
	// registered by the first and canceled by the last, because the client
	// keeps a single listener per data ID and group.
	mu       sync.Mutex
	watchers map[*func()]struct{}
}

// Option configures a Source.
type Option func(*options)

// options holds the settings collected from Options.
type options struct {
	codec encoding.Codec
}

// New returns a Source for the item dataID in group, read through client.
// An empty group means DefaultGroup. Nothing is read until Load is called.
func New(client config_client.IConfigClient, dataID, group string, opts ...Option) *Source {
	o := options{codec: jsoncodec.Codec}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if group == "" {
		group = DefaultGroup
	}
	return &Source{client: client, dataID: dataID, group: group, codec: o.codec, watchers: map[*func()]struct{}{}}
}

// WithCodec sets the codec used to decode the content, which must decode
// to an object. A nil codec keeps the default, jsoncodec.Codec.
func WithCodec(c encoding.Codec) Option {
	return func(o *options) {
		if c != nil {
			o.codec = c
		}
	}
}

// Load reads and decodes the item. Nacos reports a missing item as empty
// content, so a missing item and content containing only white space both
// yield an empty map. The client call cannot be canceled and is bounded by
// the client's own timeout; ctx is checked before it. Client failures are
// retryable errorx.Unavailable errors and undecodable content is an
// errorx.InvalidArgument error.
func (s *Source) Load(ctx context.Context) (map[string]any, error) {
	if err := s.check(ctx, "load"); err != nil {
		return nil, err
	}
	content, err := s.client.GetConfig(vo.ConfigParam{DataId: s.dataID, Group: s.group})
	if err != nil {
		return nil, s.fail(errorx.Unavailable, ReasonUnavailable, "load", err)
	}
	m := map[string]any{}
	if strings.TrimSpace(content) == "" {
		return m, nil
	}
	if err := s.codec.Unmarshal([]byte(content), &m); err != nil {
		return nil, errorx.Newf(errorx.InvalidArgument, ReasonDecode, "nacos: decode %s/%s with %s codec",
			s.group, s.dataID, s.codec.Name()).WithCause(err)
	}
	return m, nil
}

// Watch listens for changes of the item and calls notify once for every
// change the client reports. It blocks until ctx is done and then returns
// nil; notify is never called after Watch has returned. A failure to
// register the listener is returned at once as a retryable
// errorx.Unavailable error.
//
// The Nacos client keeps one listener per data ID and group and cancels
// it as a whole, so do not watch the same item through the same client
// with two Sources at the same time; concurrent Watch calls on one Source
// are fine.
func (s *Source) Watch(ctx context.Context, notify func()) error {
	if err := s.check(ctx, "watch"); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	w := &notify
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	if len(s.watchers) == 1 {
		err := s.client.ListenConfig(vo.ConfigParam{DataId: s.dataID, Group: s.group, OnChange: s.changed})
		if err != nil {
			delete(s.watchers, w)
			s.mu.Unlock()
			return s.fail(errorx.Unavailable, ReasonUnavailable, "watch", err)
		}
	}
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.watchers, w)
	if len(s.watchers) == 0 {
		// The listener is gone either way; a failure here only means the
		// client could not log the cancellation.
		_ = s.client.CancelListenConfig(vo.ConfigParam{DataId: s.dataID, Group: s.group})
	}
	return nil
}

// changed is the Nacos listener: it notifies every active Watch.
func (s *Source) changed(_, _, _, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.watchers {
		(*w)()
	}
}

// check validates the source and ctx before a client call.
func (s *Source) check(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return s.fail(errorx.From(err).Code, ReasonUnavailable, op, err)
	}
	if s.dataID == "" {
		return s.fail(errorx.InvalidArgument, ReasonInvalid, op, errors.New("empty data ID"))
	}
	return nil
}

// fail returns an errorx error describing a failed operation on the item.
func (s *Source) fail(code errorx.Code, reason, op string, err error) error {
	return errorx.Newf(code, reason, "nacos: %s %s/%s", op, s.group, s.dataID).WithCause(err)
}
