package viper

import (
	"context"
	"fmt"

	"github.com/nostosx/servex/config"
)

// Load returns one typed configuration initialized by defaults, decoded from
// sources in order, and checked through Validate() error when implemented by T
// or *T. defaults must return fresh settings; it is called once after loading.
// On failure Load returns the zero value of T. It does not watch or apply
// changes to running resources. Use New with WithValidator for reloadable views.
func Load[T any](ctx context.Context, sources []config.Source, defaults func() T, opts ...Option) (T, error) {
	var zero T
	loaded, err := New(ctx, sources, opts...)
	if err != nil {
		return zero, err
	}
	var candidate T
	if defaults != nil {
		candidate = defaults()
	}
	if err := loaded.Unmarshal("", &candidate); err != nil {
		return zero, fmt.Errorf("config: decode: %w", err)
	}
	validator, ok := any(&candidate).(interface{ Validate() error })
	if !ok {
		validator, ok = any(candidate).(interface{ Validate() error })
	}
	if ok {
		if err := validator.Validate(); err != nil {
			return zero, fmt.Errorf("config: validate: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return candidate, nil
}

// Values snapshots a nested configuration tree for explicit overrides. Dots in
// keys follow the same rules as other Sources; prefer nested maps for paths.
// Mutating the input after Values returns does not affect subsequent loads.
func Values(tree map[string]any) config.Source {
	return valueSource{tree: normalize(tree).(map[string]any)}
}

type valueSource struct{ tree map[string]any }

func (s valueSource) Load(ctx context.Context) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return normalize(s.tree).(map[string]any), nil
}
