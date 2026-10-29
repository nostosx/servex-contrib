// Package json implements encoding.Codec using encoding/json/v2.
package json

import (
	jsonv2 "encoding/json/v2"

	"github.com/nostosx/servex/encoding"
)

// Codec is a Codec named "json" backed by encoding/json/v2 with its default
// semantics. It is safe for concurrent use and must not be reassigned.
var Codec encoding.Codec = jsonCodec{}

var _ encoding.Codec = jsonCodec{}

// jsonCodec implements Codec using encoding/json/v2.
type jsonCodec struct{}

// Name returns "json".
func (jsonCodec) Name() string { return "json" }

// Marshal encodes v with jsonv2.Marshal.
func (jsonCodec) Marshal(v any) ([]byte, error) { return jsonv2.Marshal(v) }

// Unmarshal decodes data into v with jsonv2.Unmarshal.
func (jsonCodec) Unmarshal(data []byte, v any) error { return jsonv2.Unmarshal(data, v) }
