package json_test

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"strings"
	"testing"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"
)

// The same string field and ASCII payload sizes are used by the JSON, Proto
// and YAML codec benchmarks. Encoded wire sizes differ between formats.
type benchmarkPayload struct {
	Value string `json:"value"`
}

func BenchmarkCodecMarshal(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			in := &benchmarkPayload{Value: strings.Repeat("x", size)}
			data, err := jsonv2.Marshal(in)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("JSONv2", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := jsonv2.Marshal(in); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := jsoncodec.Codec.Marshal(in); err != nil {
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
			in := benchmarkPayload{Value: strings.Repeat("x", size)}
			data, err := jsonv2.Marshal(&in)
			if err != nil {
				b.Fatal(err)
			}
			var check benchmarkPayload
			if err := jsoncodec.Codec.Unmarshal(data, &check); err != nil || check != in {
				b.Fatalf("fixture round trip = %+v, %v", check, err)
			}
			b.Run("JSONv2", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out benchmarkPayload
					if err := jsonv2.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out benchmarkPayload
					if err := jsoncodec.Codec.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
