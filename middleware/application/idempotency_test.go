package application_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/storage/idempotency"
)

type idempotencyStoreFunc func(context.Context, idempotency.Key, func(context.Context) ([]byte, error)) ([]byte, error)

func (f idempotencyStoreFunc) Do(ctx context.Context, key idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	return f(ctx, key, fn)
}

func idempotencyConfig(store idempotency.Store) applicationmw.IdempotencyConfig[string, string] {
	return applicationmw.IdempotencyConfig[string, string]{
		Store: store,
		Key: func(context.Context, string) (idempotency.Key, error) {
			return idempotency.Key{Scope: "tenant-a", Operation: "untrusted-operation", Token: "caller-token", Fingerprint: [32]byte{1}}, nil
		},
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(data []byte) (string, error) { return string(data), nil },
	}
}

func TestIdempotencyReplaysResultWithOperationAndTransactionContext(t *testing.T) {
	t.Parallel()
	type transactionKey struct{}
	var saved []byte
	calls, stores := 0, 0
	store := idempotencyStoreFunc(func(ctx context.Context, key idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
		stores++
		if key.Scope != "tenant-a" || key.Operation != "order.create" || key.Token != "caller-token" || key.Fingerprint != [32]byte{1} {
			t.Fatalf("store key = %+v", key)
		}
		if ctx != t.Context() {
			t.Fatal("store lost caller context")
		}
		if saved == nil {
			data, err := fn(context.WithValue(ctx, transactionKey{}, true))
			if err != nil {
				return nil, err
			}
			saved = slices.Clone(data)
		}
		return slices.Clone(saved), nil
	})
	config := idempotencyConfig(store)
	decodes := 0
	config.Decode = func(data []byte) (string, error) {
		decodes++
		return string(data), nil
	}
	handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "order.create"},
		application.Func[string, string](func(ctx context.Context, request string) (string, error) {
			calls++
			if ctx.Value(transactionKey{}) != true || request != "input" {
				t.Fatal("business call lost transaction context or request")
			}
			return "order-42", nil
		}), applicationmw.Idempotency(config))
	for range 2 {
		if result, err := handler.Handle(t.Context(), "input"); result != "order-42" || err != nil {
			t.Fatalf("result = %q, error = %v", result, err)
		}
	}
	if calls != 1 || stores != 2 || decodes != 2 {
		t.Fatalf("business executions = %d, store calls = %d, decodes = %d", calls, stores, decodes)
	}
}

func TestIdempotencyFailureBoundaries(t *testing.T) {
	t.Parallel()
	failure := errors.New("operation failure")
	for _, tc := range []struct {
		name       string
		cause      error
		code       errorx.Code
		reason     string
		storeCalls int
		handles    int
		decodes    int
	}{
		{name: "key failure", cause: failure},
		{name: "invalid key", cause: idempotency.ErrInvalidKey, code: errorx.InvalidArgument, reason: "IDEMPOTENCY_KEY"},
		{name: "store failure", cause: failure, storeCalls: 1},
		{name: "fingerprint conflict", cause: idempotency.ErrConflict, code: errorx.AlreadyExists, reason: "IDEMPOTENCY_CONFLICT", storeCalls: 1},
		{name: "handler failure", cause: failure, storeCalls: 1, handles: 1},
		{name: "encode failure", cause: failure, storeCalls: 1, handles: 1},
		{name: "decode failure", cause: failure, storeCalls: 1, handles: 1, decodes: 1},
		{name: "new result limit", code: errorx.ResourceExhausted, reason: "IDEMPOTENCY_RESULT", storeCalls: 1, handles: 1},
		{name: "replayed result limit", code: errorx.ResourceExhausted, reason: "IDEMPOTENCY_RESULT", storeCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stores, handles, decodes, commits := 0, 0, 0, 0
			var cached []byte
			store := idempotencyStoreFunc(func(ctx context.Context, _ idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
				stores++
				switch tc.name {
				case "store failure":
					return nil, failure
				case "fingerprint conflict":
					return nil, fmt.Errorf("stored request: %w", idempotency.ErrConflict)
				case "replayed result limit":
					return []byte("too large"), nil
				}
				if cached != nil {
					return slices.Clone(cached), nil
				}
				data, err := fn(ctx)
				if err == nil {
					commits++
					cached = slices.Clone(data)
				}
				return data, err
			})
			config := idempotencyConfig(store)
			if tc.name == "key failure" {
				config.Key = func(context.Context, string) (idempotency.Key, error) { return idempotency.Key{}, failure }
			}
			if tc.name == "invalid key" {
				config.Key = func(context.Context, string) (idempotency.Key, error) { return idempotency.Key{}, nil }
			}
			if tc.name == "encode failure" {
				config.Encode = func(string) ([]byte, error) { return nil, failure }
			}
			config.Decode = func(data []byte) (string, error) {
				decodes++
				if tc.name == "decode failure" {
					return "", failure
				}
				return string(data), nil
			}
			if strings.HasSuffix(tc.name, "result limit") {
				config.MaxResultBytes = 2
			}
			handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "order.create"},
				application.Func[string, string](func(context.Context, string) (string, error) {
					handles++
					if tc.name == "handler failure" {
						return "must not publish", failure
					}
					return "result", nil
				}), applicationmw.Idempotency(config))
			for attempt := 1; attempt <= 2; attempt++ {
				result, err := handler.Handle(t.Context(), "input")
				if result != "" || err == nil || (tc.cause != nil && !errors.Is(err, tc.cause)) {
					t.Fatalf("result = %q, error = %v, cause = %v", result, err, tc.cause)
				}
				if tc.reason != "" {
					outcome := errorx.Classify(err)
					if outcome.Code != tc.code || outcome.Reason != tc.reason {
						t.Fatalf("outcome = %+v", outcome)
					}
				}
				wantHandles := tc.handles * attempt
				if tc.name == "decode failure" {
					wantHandles = 1 // The durable record exists even if the caller cannot decode it.
				}
				if stores != tc.storeCalls*attempt || handles != wantHandles || decodes != tc.decodes*attempt {
					t.Fatalf("attempt %d: stores = %d, handles = %d, decodes = %d", attempt, stores, handles, decodes)
				}
			}
			if tc.name != "decode failure" && commits != 0 {
				t.Fatal("failed execution retained a successful record")
			}
		})
	}
}

func TestIdempotencyConfigurationAndDefaultResultLimit(t *testing.T) {
	t.Parallel()
	store := idempotencyStoreFunc(func(ctx context.Context, _ idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
		return fn(ctx)
	})
	for _, name := range []string{"store", "key", "encode", "decode", "limit"} {
		t.Run(name, func(t *testing.T) {
			config := idempotencyConfig(store)
			switch name {
			case "store":
				config.Store = nil
			case "key":
				config.Key = nil
			case "encode":
				config.Encode = nil
			case "decode":
				config.Decode = nil
			case "limit":
				config.MaxResultBytes = -1
			}
			defer func() {
				if recover() == nil {
					t.Fatal("invalid configuration accepted")
				}
			}()
			applicationmw.Idempotency(config)
		})
	}
	config := idempotencyConfig(store)
	handler := application.Wrap(application.Metadata{Operation: "order.create"}, func(context.Context, string) (string, error) {
		return strings.Repeat("x", (1<<20)+1), nil
	}, applicationmw.Idempotency(config))
	if result, err := handler(t.Context(), "input"); result != "" || errorx.CodeOf(err) != errorx.ResourceExhausted {
		t.Fatalf("default limit: result length = %d, error = %v", len(result), err)
	}
}
