package gorm

import (
	"maps"

	sdkgorm "gorm.io/gorm"
)

// Option injects runtime-only native GORM behavior into an owning constructor.
type Option func(*options)

type options struct{ gorm *sdkgorm.Config }

// WithGORMConfig supplies native callbacks, plugins, naming strategy and other
// GORM options. OpenPostgres snapshots the top-level value and Plugins map;
// referenced objects must remain immutable and concurrency-safe. ConnPool and
// Dialector are rejected because OpenPostgres owns its PostgreSQL pool.
func WithGORMConfig(config *sdkgorm.Config) Option {
	return func(o *options) { o.gorm = config }
}

func newOptions(opts []Option) (options, error) {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.gorm == nil {
		return o, nil
	}
	if o.gorm.ConnPool != nil || o.gorm.Dialector != nil {
		return options{}, invalidDatabaseConfig("gorm: external pool or dialector")
	}
	copy := *o.gorm
	copy.Plugins = maps.Clone(o.gorm.Plugins)
	o.gorm = &copy
	return o, nil
}
