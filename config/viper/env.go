package viper

import (
	"context"
	"os"
	"slices"
	"strings"

	"github.com/nostosx/servex/config"
)

// Env returns a Source reading environment variables that start with prefix.
// The prefix is stripped, the remainder is lower-cased and "__" separates
// nesting levels: with prefix "APP_", APP_BROKER__KAFKA__BROKERS=a yields
// broker.kafka.brokers = "a". Values are always strings. Variables whose
// name has an empty segment are ignored, and when a name is both a value and
// a parent of other names the nested object wins.
func Env(prefix string) config.Source { return envSource{prefix: prefix} }

// envSource implements Env.
type envSource struct{ prefix string }

// Load snapshots the environment.
func (e envSource) Load(context.Context) (map[string]any, error) {
	root := map[string]any{}
	env := os.Environ()
	slices.Sort(env)
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		rest, ok := strings.CutPrefix(name, e.prefix)
		if !ok || rest == "" {
			continue
		}
		path := strings.Split(strings.ToLower(rest), "__")
		if slices.Contains(path, "") {
			continue
		}
		setEnv(root, path, value)
	}
	return root, nil
}

// setEnv stores value at path, letting objects take precedence over scalars
// so the result does not depend on variable order.
func setEnv(m map[string]any, path []string, value string) {
	last := len(path) - 1
	for _, seg := range path[:last] {
		child, ok := m[seg].(map[string]any)
		if !ok {
			child = map[string]any{}
			m[seg] = child
		}
		m = child
	}
	if _, isMap := m[path[last]].(map[string]any); !isMap {
		m[path[last]] = value
	}
}
