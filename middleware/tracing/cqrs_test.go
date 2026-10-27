package tracing_test

import (
	"context"
	"sync"
	"testing"

	mwtracing "github.com/nostosx/servex-contrib/middleware/tracing"
	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

func TestTypedTracing(t *testing.T) {
	t.Parallel()
	for _, kind := range []cqrs.Kind{cqrs.KindCommand, cqrs.KindQuery} {
		t.Run(string(kind), func(t *testing.T) {
			tr := &recordingTracer{}
			parentCtx, parent := tr.Start(t.Context(), "parent", tracing.SpanKindServer)
			defer parent.End()
			header := metadata.Pairs("traceparent", "untrusted-header")
			ctx, cancel := context.WithCancel(metadata.NewContext(parentCtx, metadata.Info{Kind: metadata.KindClient, Protocol: "http", Operation: "transport", Header: header}))
			cancel()
			failure := errorx.New(errorx.Internal, "PRIVATE", "secret-token")
			base := cqrs.CommandFunc[int, int](func(got context.Context, req int) (int, error) {
				info, _ := metadata.FromContext(got)
				if got.Err() != context.Canceled || info.Operation != "transport" || tracing.SpanFromContext(got) == parent {
					t.Error("context or transport metadata changed")
				}
				return req, failure
			})
			var handler cqrs.Handler[int, int]
			if kind == cqrs.KindCommand {
				handler = cqrs.ChainCommand("catalog.create", base, mwtracing.Command[int, int](tr))
			} else {
				handler = cqrs.ChainQuery("catalog.get", base, mwtracing.Query[int, int](tr))
			}
			got, err := handler.Handle(ctx, 7)
			if got != 7 || err != failure {
				t.Fatal("result or error changed")
			}
			span := tr.spans[1]
			if span.kind != tracing.SpanKindInternal || span.parent != parent.SpanID() || span.traceID != parent.TraceID() || span.ended != 1 || span.attrs[mwtracing.AttrKind] != string(kind) || span.attrs[mwtracing.AttrOperation] != span.name || span.attrs[mwtracing.AttrCode] != "Internal" {
				t.Fatalf("span: %+v", span)
			}
			if len(span.errs) != 1 || span.errs[0].Error() != "Internal" || len(span.attrs) != 3 || header.Get("traceparent") != "untrusted-header" {
				t.Fatal("sensitive error or transport header leaked/changed")
			}
		})
	}
}

func TestTypedTracingErrorStatusByCodeClass(t *testing.T) {
	t.Parallel()
	for code := errorx.Canceled; code <= errorx.Unauthenticated; code++ {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			tr := &recordingTracer{}
			failure := errorx.New(code, "PRIVATE", "secret-token")
			handler := cqrs.ChainQuery("catalog.get", cqrs.QueryFunc[int, int](func(context.Context, int) (int, error) {
				return 0, failure
			}), mwtracing.Query[int, int](tr))
			if _, err := handler.Handle(t.Context(), 1); err != failure {
				t.Fatalf("err = %v, want unchanged", err)
			}
			span := tr.only(t)
			if span.attrs[mwtracing.AttrCode] != code.String() || span.ended != 1 {
				t.Fatalf("span: %+v", span)
			}
			want := serverFaults[code]
			if want != (len(span.errs) == 1) || len(span.errs) > 1 {
				t.Fatalf("errs = %v, want error status %v", span.errs, want)
			}
			if want && span.errs[0].Error() != code.String() {
				t.Fatalf("recorded %q, want the code only", span.errs[0])
			}
		})
	}
}

func TestTypedTracingPanic(t *testing.T) {
	t.Parallel()
	tr := &recordingTracer{}
	marker := &struct{ secret string }{"private-panic"}
	handler := cqrs.ChainCommand("catalog.create", cqrs.CommandFunc[int, int](func(context.Context, int) (int, error) { panic(marker) }), mwtracing.Command[int, int](tr))
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic changed")
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	span := tr.only(t)
	if span.ended != 1 || span.attrs[mwtracing.AttrCode] != "Internal" || len(span.errs) != 1 || span.errs[0].Error() != "Internal" {
		t.Fatalf("panic span: %+v", span)
	}
}

func TestTypedTracingConcurrent(t *testing.T) {
	t.Parallel()
	tr := &recordingTracer{}
	base := cqrs.QueryFunc[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := cqrs.ChainQuery("catalog.get", base, mwtracing.Query[int, int](tr))
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
	if len(tr.spans) != 32 {
		t.Fatal("lost concurrent span")
	}
	for _, span := range tr.spans {
		if span.ended != 1 || span.attrs[mwtracing.AttrCode] != "OK" || len(span.errs) != 0 {
			t.Fatal("bad successful span")
		}
	}
	if got, err := cqrs.ChainQuery("catalog.get", base, mwtracing.Query[int, int](nil)).Handle(t.Context(), 3); got != 3 || err != nil {
		t.Fatal("nil tracer changed call")
	}
}
