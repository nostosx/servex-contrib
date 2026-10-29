package yaml_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	yamlv3 "go.yaml.in/yaml/v3"

	"github.com/nostosx/servex-contrib/encoding/yaml"
)

type benchmarkPayload struct {
	Value string `yaml:"value"`
}

func BenchmarkCodecMarshal(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			in := &benchmarkPayload{Value: strings.Repeat("x", size)}
			data, err := yamlv3.Marshal(in)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("YAMLv3", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := yamlv3.Marshal(in); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					if _, err := yaml.Codec.Marshal(in); err != nil {
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
			data, err := yamlv3.Marshal(&in)
			if err != nil {
				b.Fatal(err)
			}
			var check benchmarkPayload
			if err := yaml.Codec.Unmarshal(data, &check); err != nil || check != in {
				b.Fatalf("fixture round trip = %+v, %v", check, err)
			}
			b.Run("YAMLv3", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out benchmarkPayload
					if err := yamlv3.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out benchmarkPayload
					if err := yaml.Codec.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// String-only keys and values keep raw YAML and normalized adapter outputs
// semantically identical, while exposing the adapter's dynamic traversal cost.
func BenchmarkCodecDynamicMapUnmarshal(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			data := []byte("value: " + strings.Repeat("x", size) + "\nnested: {name: alice, role: reader}\n")
			var raw, normalized map[string]any
			if err := yamlv3.Unmarshal(data, &raw); err != nil {
				b.Fatal(err)
			}
			if err := yaml.Codec.Unmarshal(data, &normalized); err != nil || !reflect.DeepEqual(raw, normalized) {
				b.Fatalf("fixture normalization changed values: %v", err)
			}
			b.Run("YAMLv3", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out map[string]any
					if err := yamlv3.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Servex", func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out map[string]any
					if err := yaml.Codec.Unmarshal(data, &out); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
