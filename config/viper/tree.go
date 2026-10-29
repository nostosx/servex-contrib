package viper

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// normalize returns a fresh copy of v in canonical form: maps with any key
// type become map[string]any (keys formatted with fmt), slices and arrays
// other than byte slices become []any and non-nil pointers are dereferenced.
// Other values are returned unchanged.
func normalize(v any) any {
	switch x := v.(type) {
	case nil, string, bool, float64, int, int64:
		return x
	case map[string]any:
		m := make(map[string]any, len(x))
		for _, k := range slices.Sorted(maps.Keys(x)) {
			m[k] = normalize(x[k])
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = normalize(e)
		}
		return s
	case []byte:
		return bytes.Clone(x)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		entries := make(map[string]any, rv.Len())
		for k, e := range rv.Seq2() {
			var key string
			if k.Kind() == reflect.String {
				key = k.String()
			} else {
				key = fmt.Sprint(k.Interface())
			}
			entries[key] = e.Interface()
		}
		return normalize(entries)
	case reflect.Slice:
		if rv.IsNil() {
			return []any{}
		}
		fallthrough
	case reflect.Array:
		s := make([]any, rv.Len())
		for i := range rv.Len() {
			s[i] = normalize(rv.Index(i).Interface())
		}
		return s
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return normalize(rv.Elem().Interface())
	}
	return v
}

// clone deep-copies the container parts of a normalized value.
func clone(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = clone(e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = clone(e)
		}
		return s
	case []byte:
		return bytes.Clone(x)
	}
	return v
}

// findKey returns the key of m that matches k, preferring an exact match and
// falling back to a case-insensitive one. Merged maps never hold two keys
// that are equal under case folding, so the fallback is unambiguous there.
func findKey(m map[string]any, k string) (string, bool) {
	if _, ok := m[k]; ok {
		return k, true
	}
	for key := range m {
		if strings.EqualFold(key, k) {
			return key, true
		}
	}
	return "", false
}

// lookup resolves a dotted path in a normalized tree. The empty path
// resolves to root itself. Numeric segments index into lists.
func lookup(root any, path string) (any, bool) {
	if path == "" {
		return root, true
	}
	cur := root
	for seg := range strings.SplitSeq(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			k, ok := findKey(node, seg)
			if !ok {
				return nil, false
			}
			cur = node[k]
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}
