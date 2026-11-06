package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	applicationmiddleware "github.com/nostosx/servex-contrib/middleware/application"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/storage/cache"
)

type store struct {
	data    []byte
	failure error
}

func (s *store) Get(context.Context, string) ([]byte, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	if s.data == nil {
		return nil, cache.ErrNotFound
	}
	return append([]byte(nil), s.data...), nil
}
func (s *store) Set(_ context.Context, _ string, data []byte, _ time.Duration) error {
	s.data = append([]byte(nil), data...)
	return s.failure
}
func (s *store) Delete(context.Context, string) error { s.data = nil; return nil }

func TestCacheOnlyQueriesAndExplicitFailurePolicy(t *testing.T) {
	t.Parallel()
	s := &store{}
	calls := 0
	config := applicationmiddleware.QueryCacheConfig[string, string]{Store: s, Key: func(_ context.Context, in string) (string, error) { return "tenant:" + in, nil }, Encode: func(value string) ([]byte, error) { return []byte(value), nil }, Decode: func(value []byte) (string, error) { return string(value), nil }, TTL: time.Minute}
	handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, application.Func[string, string](func(context.Context, string) (string, error) { calls++; return "value", nil }), applicationmiddleware.QueryCache(config))
	for range 2 {
		if value, err := handler.Handle(t.Context(), "key"); value != "value" || err != nil {
			t.Fatalf("result=%q error=%v", value, err)
		}
	}
	if calls != 1 {
		t.Fatalf("cache missed: %d executions", calls)
	}
	s.failure = errors.New("cache down")
	if _, err := handler.Handle(t.Context(), "key"); !errors.Is(err, s.failure) || calls != 1 {
		t.Fatal("cache failed open without opt-in")
	}
	config.FailOpen = true
	open := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, application.Func[string, string](func(context.Context, string) (string, error) { calls++; return "fresh", nil }), applicationmiddleware.QueryCache(config))
	if value, err := open.Handle(t.Context(), "key"); value != "fresh" || err != nil || calls != 2 {
		t.Fatal("explicit fallback failed")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("command caching must be rejected")
		}
	}()
	application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create"}, application.Func[string, string](func(context.Context, string) (string, error) { return "", nil }), applicationmiddleware.QueryCache(config))
}
