package etcd

import (
	"fmt"
	"strings"
)

// segments splits a relative key into its non-empty "/"-separated parts.
func segments(rel string) []string {
	var parts []string
	for p := range strings.SplitSeq(rel, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// insert stores v at path in root, creating intermediate objects and
// replacing non-object values in the way. An empty path merges v, which
// must then be an object, into root.
func insert(root map[string]any, path []string, v any) error {
	if len(path) == 0 {
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("value at the prefix must be an object, got %T", v)
		}
		merge(root, m)
		return nil
	}
	node := root
	for _, seg := range path[:len(path)-1] {
		child, ok := node[seg].(map[string]any)
		if !ok {
			child = map[string]any{}
			node[seg] = child
		}
		node = child
	}
	put(node, path[len(path)-1], v)
	return nil
}

// put stores v under k in dst, merging recursively when both the old and
// the new value are objects.
func put(dst map[string]any, k string, v any) {
	if old, ok := dst[k].(map[string]any); ok {
		if m, ok := v.(map[string]any); ok {
			merge(old, m)
			return
		}
	}
	dst[k] = v
}

// merge deep-merges src into dst; src must not be used afterwards.
func merge(dst, src map[string]any) {
	for k, v := range src {
		put(dst, k, v)
	}
}
