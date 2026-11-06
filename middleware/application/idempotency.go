// Idempotency middleware decorates business handlers with an injected durable
// store. Keys, authenticated scope, canonical fingerprints and codecs are explicit.
package application

import (
	"context"
	"errors"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	storeapi "github.com/nostosx/servex/storage/idempotency"
)

type IdempotencyConfig[Request, Result any] struct {
	Store  storeapi.Store
	Key    func(context.Context, Request) (storeapi.Key, error)
	Encode func(Result) ([]byte, error)
	Decode func([]byte) (Result, error)
	// MaxResultBytes bounds both new and replayed results; zero selects 1 MiB.
	MaxResultBytes int
}

func Idempotency[Request, Result any](config IdempotencyConfig[Request, Result]) application.Middleware[Request, Result] {
	if config.Store == nil || config.Key == nil || config.Encode == nil || config.Decode == nil || config.MaxResultBytes < 0 {
		panic("idempotency: invalid configuration")
	}
	if config.MaxResultBytes == 0 {
		config.MaxResultBytes = 1 << 20
	}
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			var zero Result
			key, err := config.Key(ctx, request)
			if err != nil {
				return zero, err
			}
			key.Operation = meta.Operation
			if err := key.Validate(); err != nil {
				return zero, errorx.New(errorx.InvalidArgument, "IDEMPOTENCY_KEY", "invalid idempotency key").WithCause(err)
			}
			encoded, err := config.Store.Do(ctx, key, func(txctx context.Context) ([]byte, error) {
				result, err := next.Handle(txctx, request)
				if err != nil {
					return nil, err
				}
				data, err := config.Encode(result)
				if err != nil {
					return nil, err
				}
				if len(data) > config.MaxResultBytes {
					return nil, errorx.New(errorx.ResourceExhausted, "IDEMPOTENCY_RESULT", "idempotency result exceeds limit")
				}
				return data, nil
			})
			if errors.Is(err, storeapi.ErrConflict) {
				return zero, errorx.New(errorx.AlreadyExists, "IDEMPOTENCY_CONFLICT", "idempotency key used with different input").WithCause(err)
			}
			if err != nil {
				return zero, err
			}
			if len(encoded) > config.MaxResultBytes {
				return zero, errorx.New(errorx.ResourceExhausted, "IDEMPOTENCY_RESULT", "idempotency result exceeds limit")
			}
			return config.Decode(encoded)
		})
	}
}
