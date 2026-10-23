package proto

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/errorx"
)

// Compile-time interface check.
var _ encoding.Codec = codec{}

// Codec is a Codec named "proto" backed by google.golang.org/protobuf/proto.
// Values passed to Marshal and Unmarshal must implement proto.Message; other
// values yield an errorx.InvalidArgument error. Codec is safe for concurrent
// use and must not be reassigned.
var Codec encoding.Codec = codec{}

// codec implements encoding.Codec for Protocol Buffers messages.
type codec struct{}

// Name returns "proto".
func (codec) Name() string { return "proto" }

// Marshal encodes v, which must implement proto.Message, in the binary wire
// format.
func (codec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, notMessage("marshal", v)
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, errorx.New(errorx.InvalidArgument, "", fmt.Sprintf("proto: marshal %T", v)).WithCause(err)
	}
	return data, nil
}

// Unmarshal decodes data into v, which must be a non-nil proto.Message. Any
// existing content of v is reset first.
func (codec) Unmarshal(data []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return notMessage("unmarshal", v)
	}
	if !m.ProtoReflect().IsValid() {
		return errorx.New(errorx.InvalidArgument, "", fmt.Sprintf("proto: unmarshal into nil %T", v))
	}
	if err := proto.Unmarshal(data, m); err != nil {
		return errorx.New(errorx.InvalidArgument, "", fmt.Sprintf("proto: unmarshal %T", v)).WithCause(err)
	}
	return nil
}

// notMessage reports a value that does not implement proto.Message.
func notMessage(op string, v any) error {
	return errorx.New(errorx.InvalidArgument, "", fmt.Sprintf("proto: %s: %T does not implement proto.Message", op, v))
}
