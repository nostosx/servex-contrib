package file

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	upstream "github.com/spf13/viper"

	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/encoding"
)

// defaultPollInterval is the default interval between file checks.
const defaultPollInterval = 2 * time.Second

// Compile-time checks that Source satisfies the config contracts.
var (
	_ config.Source    = (*Source)(nil)
	_ config.Watchable = (*Source)(nil)
)

// Source reads configuration from a single file. It implements
// config.Source and config.Watchable and is safe for concurrent use when its
// codecs are. Create a Source with New; its zero value is not usable.
type Source struct {
	path     string
	codecs   map[string]encoding.Codec
	interval time.Duration
}

// Option configures a Source.
type Option func(*options)

// options holds the settings collected from Options.
type options struct {
	codecs   map[string]encoding.Codec
	interval time.Duration
}

// New returns a Source for the file at path. The file is not read until
// Load is called. Relative paths remain relative to the working directory at
// each Load or Watch poll; use an absolute path to pin the location.
func New(path string, opts ...Option) *Source {
	o := options{
		codecs:   builtinCodecs(),
		interval: defaultPollInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Source{path: path, codecs: maps.Clone(o.codecs), interval: o.interval}
}

// WithCodec registers c for files with extension ext, including the dot, as
// in ".yaml". Extensions are matched case-insensitively and a missing dot is
// added. A nil codec removes the mapping. By default Viper handles its supported file formats.
func WithCodec(ext string, c encoding.Codec) Option {
	ext = strings.ToLower(ext)
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	return func(o *options) {
		if c == nil {
			delete(o.codecs, ext)
			return
		}
		o.codecs[ext] = c
	}
}

// WithPollInterval sets how often Watch checks the file's modification time
// and size. Non-positive values keep the default of 2s.
func WithPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.interval = d
		}
	}
}

// Load reads and decodes the file. A file containing only white space
// yields an empty map.
func (s *Source) Load(ctx context.Context) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("file: load %s: %w", s.path, err)
	}
	ext := strings.ToLower(filepath.Ext(s.path))
	codec, ok := s.codecs[ext]
	if !ok {
		return nil, fmt.Errorf("file: load %s: no codec for extension %q", s.path, ext)
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("file: load %s: %w", s.path, err)
	}
	m := map[string]any{}
	if len(bytes.TrimSpace(data)) == 0 {
		return m, nil
	}
	if codec == nil {
		v := upstream.New()
		v.SetConfigType(strings.TrimPrefix(ext, "."))
		if err := v.ReadConfig(bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("file: decode %s: %w", s.path, err)
		}
		return v.AllSettings(), nil
	}
	if err := codec.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("file: decode %s with %s codec: %w", s.path, codec.Name(), err)
	}
	return m, nil
}

// Watch polls the file every poll interval and calls notify whenever its
// identity, modification time or size differs from the previous check while
// the file exists. A missing file is not reported, but its reappearance is.
// Watch returns nil once ctx is done.
func (s *Source) Watch(ctx context.Context, notify func()) error {
	prev := s.stat()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		cur := s.stat()
		if cur.changedFrom(prev) && cur.info != nil {
			notify()
		}
		prev = cur
	}
}

// state is the observed state of the file at one poll.
type state struct {
	info fs.FileInfo // nil when the file cannot be stat'ed
}

// stat observes the file.
func (s *Source) stat() state {
	info, err := os.Stat(s.path)
	if err != nil {
		return state{}
	}
	return state{info: info}
}

// changedFrom reports whether st differs from prev.
func (st state) changedFrom(prev state) bool {
	switch {
	case st.info == nil || prev.info == nil:
		return (st.info == nil) != (prev.info == nil)
	case !os.SameFile(st.info, prev.info):
		return true
	}
	return !st.info.ModTime().Equal(prev.info.ModTime()) || st.info.Size() != prev.info.Size()
}

// builtinCodecs reserves formats with decoders in the pinned Viper version. A nil entry selects
// Viper decoding; custom non-nil codecs override one extension explicitly.
func builtinCodecs() map[string]encoding.Codec {
	codecs := make(map[string]encoding.Codec)
	for _, ext := range []string{"json", "yaml", "yml", "toml", "env", "dotenv"} {
		codecs["."+ext] = nil
	}
	return codecs
}
