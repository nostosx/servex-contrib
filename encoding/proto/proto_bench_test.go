package proto_test

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	protocodec "github.com/nostosx/servex-contrib/encoding/proto"
)

// StringValue uses generated protobuf code for the same single value field
// and ASCII payload sizes used by the JSON and YAML codec benchmarks.
func BenchmarkCodecMarshal(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			in := wrapperspb.String(strings.Repeat("x", size))
			data, err := proto.Marshal(in)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Protobuf", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := proto.Marshal(in); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := protocodec.Codec.Marshal(in); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkCodecUnmarshal(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			in := wrapperspb.String(strings.Repeat("x", size))
			data, err := proto.Marshal(in)
			if err != nil {
				b.Fatal(err)
			}
			var check wrapperspb.StringValue
			if err := protocodec.Codec.Unmarshal(data, &check); err != nil || check.GetValue() != in.GetValue() {
				b.Fatalf("fixture round trip = %+v, %v", &check, err)
			}
			b.Run("Protobuf", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out wrapperspb.StringValue
					if err := proto.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out wrapperspb.StringValue
					if err := protocodec.Codec.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
