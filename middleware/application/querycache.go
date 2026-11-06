// QueryCache middleware decorates read-only queries with an explicitly scoped cache.
// It owns no invalidation, consistency guarantees or stampede coordination.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/storage/cache"
)

type QueryCacheConfig[Request, Result any] struct {
	Store  cache.Cache
	Key    func(context.Context, Request) (string, error)
	Encode func(Result) ([]byte, error)
	Decode func([]byte) (Result, error)
	TTL    time.Duration
	// FailOpen explicitly permits backend or corrupt-entry fallback to the query.
	FailOpen bool
}

func QueryCache[Request, Result any](config QueryCacheConfig[Request, Result]) application.Middleware[Request, Result] {
	if config.Store == nil || config.Key == nil || config.Encode == nil || config.Decode == nil || config.TTL <= 0 {
		panic("querycache: invalid configuration")
	}
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		if meta.Kind != application.KindQuery {
			panic("querycache: read-only query required")
		}
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			var zero Result
			key, err := config.Key(ctx, request)
			if err != nil {
				return zero, err
			}
			if key == "" {
				return zero, errors.New("querycache: empty scoped key")
			}
			key = meta.Operation + "\x00" + key
			data, err := config.Store.Get(ctx, key)
			if err == nil {
				result, decodeErr := config.Decode(data)
				if decodeErr == nil {
					return result, nil
				}
				if !config.FailOpen {
					return zero, decodeErr
				}
			} else if !errors.Is(err, cache.ErrNotFound) && !config.FailOpen {
				return zero, err
			}
			if ctx.Err() != nil {
				return zero, errors.Join(ctx.Err(), context.Cause(ctx))
			}
			result, err := next.Handle(ctx, request)
			if err != nil {
				return result, err
			}
			data, err = config.Encode(result)
			if err != nil {
				return zero, err
			}
			if err = config.Store.Set(ctx, key, data, config.TTL); err != nil && !config.FailOpen {
				return zero, err
			}
			return result, nil
		})
	}
}
