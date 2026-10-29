package viper

import (
	"fmt"
	"reflect"
	"time"

	"github.com/spf13/cast"

	"github.com/go-viper/mapstructure/v2"
	upstream "github.com/spf13/viper"

	"github.com/nostosx/servex/config"
)

// Snapshot holds one private, immutable tree accepted by Viper. Its zero value is empty.
// Accessors use Viper conversions; decoding uses json tags and mapstructure
// weak conversions, embedded-field squashing and TextUnmarshaler support.
type Snapshot struct{ data map[string]any }

var _ config.Reader = (*Snapshot)(nil)

// Value returns an isolated value; an empty path returns the whole tree.
func (s *Snapshot) Value(path string) (any, bool) {
	if path == "" {
		if s.data == nil {
			return map[string]any{}, true
		}
		return clone(s.data), true
	}
	value, ok := lookup(s.data, path)
	return clone(value), ok
}

// raw reads the immutable tree without exposing it to callers.
func (s *Snapshot) raw(path string) any { value, _ := lookup(s.data, path); return value }

// String reads a string using Viper conversion rules.
func (s *Snapshot) String(path string) string { return cast.ToString(s.raw(path)) }

// Int reads an integer using Viper conversion rules.
func (s *Snapshot) Int(path string) int { return cast.ToInt(s.raw(path)) }

// Bool reads a boolean using Viper conversion rules.
func (s *Snapshot) Bool(path string) bool { return cast.ToBool(s.raw(path)) }

// Float64 reads a floating-point value using Viper conversion rules.
func (s *Snapshot) Float64(path string) float64 { return cast.ToFloat64(s.raw(path)) }

// Duration accepts duration strings and numeric nanoseconds.
func (s *Snapshot) Duration(path string) time.Duration { return cast.ToDuration(s.raw(path)) }

// Unmarshal decodes a copied value so destination containers cannot mutate
// published configuration. Missing paths leave the destination unchanged.
func (s *Snapshot) Unmarshal(path string, dst any) error {
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("config: decode %q: destination must be a non-nil pointer", path)
	}
	value, ok := s.Value(path)
	if !ok {
		return nil
	}
	local := upstream.New()
	local.Set("value", value)
	return local.UnmarshalKey("value", dst, func(c *mapstructure.DecoderConfig) {
		c.TagName = "json"
		c.Squash = true
		c.ZeroFields = false
		c.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.TextUnmarshallerHookFunc(),
		)
	})
}
