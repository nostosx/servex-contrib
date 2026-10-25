package logging_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/logging"
	mwmetrics "github.com/nostosx/servex-contrib/middleware/metrics"
	mwtracing "github.com/nostosx/servex-contrib/middleware/tracing"
	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
)

func TestTypedLogging(t *testing.T) {
	t.Parallel()
	for _, kind := range []cqrs.Kind{cqrs.KindCommand, cqrs.KindQuery} {
		t.Run(string(kind), func(t *testing.T) {
			sink := &captureHandler{}
			failure := errors.New("secret-token")
			ctx, cancel := context.WithCancel(serverCtx(t.Context()))
			cancel()
			base := cqrs.CommandFunc[string, string](func(got context.Context, req string) (string, error) {
				if got != ctx || got.Err() != context.Canceled || req != "private-payload" {
					t.Error("call was changed")
				}
				return "private-result", failure
			})
			var handler cqrs.Handler[string, string]
			if kind == cqrs.KindCommand {
				handler = cqrs.ChainCommand("catalog.create", base, logging.Command[string, string](slog.New(sink)))
			} else {
				handler = cqrs.ChainQuery("catalog.get", base, logging.Query[string, string](slog.New(sink)))
			}
			got, err := handler.Handle(ctx, "private-payload")
			if got != "private-result" || err != failure {
				t.Fatal("result or error identity changed")
			}
			record, attrs := sink.only(t)
			if attrs[logging.KeyKind].String() != string(kind) || attrs[logging.KeyCode].String() != "Unknown" || !strings.HasPrefix(attrs[logging.KeyOperation].String(), "catalog.") || record.Level != slog.LevelError {
				t.Fatalf("record: %v", attrs)
			}
			if len(attrs) != 4 {
				t.Fatalf("unexpected attributes: %v", attrs)
			}
			for _, value := range attrs {
				if strings.Contains(value.String(), "private") || strings.Contains(value.String(), "secret") {
					t.Fatalf("leaked value: %v", attrs)
				}
			}
		})
	}
}

func TestTypedLoggingPanic(t *testing.T) {
	t.Parallel()
	sink := &captureHandler{}
	marker := &struct{ secret string }{"do-not-log"}
	handler := cqrs.ChainCommand("catalog.create", cqrs.CommandFunc[int, int](func(context.Context, int) (int, error) { panic(marker) }), logging.Command[int, int](slog.New(sink)))
	func() {
		defer func() {
			if got := recover(); got != marker {
				t.Errorf("panic changed: %v", got)
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	_, attrs := sink.only(t)
	if attrs[logging.KeyCode].String() != "Internal" || len(attrs) != 4 {
		t.Fatalf("panic record: %v", attrs)
	}
}

func TestTypedLoggingDisabledAndConcurrent(t *testing.T) {
	t.Parallel()
	sink := &captureHandler{min: slog.LevelError}
	base := cqrs.QueryFunc[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := cqrs.ChainQuery("catalog.get", base, logging.Query[int, int](slog.New(sink), nil, logging.WithLevel(func(errorx.Code) slog.Level { return slog.LevelDebug })))
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			got, err := handler.Handle(t.Context(), i)
			if got != i || err != nil {
				t.Error("changed call")
			}
		})
	}
	wg.Wait()
	if len(sink.records) != 0 {
		t.Fatal("disabled logger recorded calls")
	}
	if got, err := cqrs.ChainQuery("catalog.get", base, logging.Query[int, int](nil)).Handle(t.Context(), 2); got != 2 || err != nil {
		t.Fatal("nil logger changed call")
	}
}

func ExampleCommand() {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	create := cqrs.ChainCommand("catalog.create_product",
		cqrs.CommandFunc[string, string](func(_ context.Context, name string) (string, error) { return name, nil }),
		mwtracing.Command[string, string](nil), mwmetrics.Command[string, string](nil), logging.Command[string, string](logger),
	)
	get := cqrs.ChainQuery("catalog.get_product",
		cqrs.QueryFunc[string, string](func(_ context.Context, id string) (string, error) { return id, nil }),
		mwtracing.Query[string, string](nil), mwmetrics.Query[string, string](nil), logging.Query[string, string](logger),
	)
	created, err := create.Handle(context.Background(), "book")
	fmt.Println(created, err)
	found, err := get.Handle(context.Background(), created)
	fmt.Println(found, err)
	// Output:
	// book <nil>
	// book <nil>
}
