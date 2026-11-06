package loader

import (
	"context"
	jsonv2 "encoding/json/v2"
	"time"

	"github.com/nostosx/servex/storage/cache"
)

// testJSONCodec supplies JSON encoding for Loader regression fixtures.
type testJSONCodec struct{}

func (testJSONCodec) Name() string                         { return "json" }
func (testJSONCodec) Marshal(v any) ([]byte, error)        { return jsonv2.Marshal(v) }
func (testJSONCodec) Unmarshal(data []byte, dst any) error { return jsonv2.Unmarshal(data, dst) }

func newLoader[T any](ctx context.Context, c cache.Cache, timeout time.Duration, opts ...Option) (*Loader[T], error) {
	return New[T](ctx, c, timeout, append([]Option{WithCodec(testJSONCodec{})}, opts...)...)
}
