package yaml

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	yamlv3 "go.yaml.in/yaml/v3"

	"github.com/nostosx/servex/encoding"
)

// Codec is a Codec named "yaml" backed by go.yaml.in/yaml/v3. Mappings
// decoded into dynamically typed values (any, map[string]any, []any and
// fields of those types) are always map[string]any: non-string keys are
// formatted with fmt.Sprint, and timestamps become RFC 3339 strings so
// they can be read like any other scalar. Codec is safe for concurrent use and
// must not be reassigned.
var Codec encoding.Codec = codec{}

// Compile-time check that codec satisfies encoding.Codec.
var _ encoding.Codec = codec{}

// errNilPointer reports an Unmarshal destination that cannot be written.
var errNilPointer = errors.New("yaml: unmarshal destination must be a non-nil pointer")

// codec implements encoding.Codec using go.yaml.in/yaml/v3.
type codec struct{}

// Name returns "yaml".
func (codec) Name() string { return "yaml" }

// Marshal encodes v with yaml.Marshal.
func (codec) Marshal(v any) (data []byte, err error) {
	// yaml.Marshal panics on some unsupported values instead of returning
	// an error; turn those panics into errors.
	defer func() {
		if r := recover(); r != nil {
			data, err = nil, fmt.Errorf("yaml: marshal %T: %v", v, r)
		}
	}()
	return yamlv3.Marshal(v)
}

// Unmarshal decodes the first document in data into v, which must be a
// non-nil pointer. An empty document leaves v unchanged.
func (codec) Unmarshal(data []byte, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("%w, got %T", errNilPointer, v)
	}
	if err := yamlv3.Unmarshal(data, v); err != nil {
		return err
	}
	stringKeys(rv.Elem())
	return nil
}

// stringKeys rewrites, in place, every dynamically typed value reachable
// from rv as described by normalize.
func stringKeys(rv reflect.Value) {
	switch rv.Kind() {
	case reflect.Pointer:
		if !rv.IsNil() {
			stringKeys(rv.Elem())
		}
	case reflect.Interface:
		if rv.IsNil() || !rv.CanSet() {
			return
		}
		if n, ok := normalize(rv.Elem().Interface()); ok {
			rv.Set(reflect.ValueOf(n))
		}
	case reflect.Struct:
		for _, f := range rv.Fields() {
			if f.CanSet() {
				stringKeys(f)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range rv.Len() {
			stringKeys(rv.Index(i))
		}
	case reflect.Map:
		if rv.IsNil() || !holdsDynamic(rv.Type().Elem()) {
			return
		}
		for k, e := range rv.Seq2() {
			// Map elements are not addressable: fix a copy and store it.
			c := reflect.New(e.Type()).Elem()
			c.Set(e)
			stringKeys(c)
			rv.SetMapIndex(k, c)
		}
	}
}

// holdsDynamic reports whether values of type t may contain an interface
// value, and therefore a map decoded with non-string keys.
func holdsDynamic(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Struct:
		return true
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return holdsDynamic(t.Elem())
	}
	return false
}

// normalize converts the generic values produced by the YAML decoder,
// recursively, so every map is a map[string]any and every timestamp is an
// RFC 3339 string. It reports whether v itself was replaced.
func normalize(v any) (any, bool) {
	switch x := v.(type) {
	case time.Time:
		return x.Format(time.RFC3339Nano), true
	case map[any]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			if n, ok := normalize(e); ok {
				e = n
			}
			m[keyString(k)] = e
		}
		return m, true
	case map[string]any:
		for k, e := range x {
			if n, ok := normalize(e); ok {
				x[k] = n
			}
		}
	case []any:
		for i, e := range x {
			if n, ok := normalize(e); ok {
				x[i] = n
			}
		}
	}
	return v, false
}

// keyString formats a decoded mapping key.
func keyString(k any) string {
	if s, ok := k.(string); ok {
		return s
	}
	return fmt.Sprint(k)
}
