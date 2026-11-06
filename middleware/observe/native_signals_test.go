package observe_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"

	clientmw "github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
	jobmw "github.com/nostosx/servex-contrib/middleware/job"
	messagingmw "github.com/nostosx/servex-contrib/middleware/messaging"

	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
	metricapi "github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
	"github.com/nostosx/servex/transport"
)

func TestAttemptSignalsAreIndependentlySelectable(t *testing.T) {
	for _, mode := range []string{"logging", "metrics", "tracing"} {
		t.Run(mode, func(t *testing.T) {
			r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
			parent := &eventRecording{attrs: map[string]any{}}
			ctx := tracing.ContextWithSpan(t.Context(), parent)
			ctx = metadata.NewContext(ctx, metadata.Info{Operation: "catalog.get", Header: metadata.Pairs("x-trace", "original")})
			var middleware transport.AttemptMiddleware
			switch mode {
			case "logging":
				middleware = clientmw.AttemptLogging(r)
			case "metrics":
				middleware = clientmw.AttemptMetrics(r)
			case "tracing":
				middleware = clientmw.AttemptTracing(r, propagator{})
			}
			cause := errors.New("private transport failure")
			calls := 0
			invoker := middleware(transport.InvokerFunc(func(got context.Context, address string, request any) (any, error) {
				calls++
				if address != "node:80" || request != 7 {
					t.Fatal("native call changed")
				}
				expected := tracing.Span(parent)
				if mode == "tracing" {
					expected = r
				} else if got != ctx {
					t.Fatal("logging or metrics replaced caller context")
				}
				if tracing.SpanFromContext(got) != expected {
					t.Fatal("wrong active span")
				}
				return 9, cause
			}))
			result, err := invoker.Invoke(ctx, "node:80", 7)
			if result != 9 || err != cause || calls != 1 {
				t.Fatalf("result=%v err=%v calls=%d", result, err, calls)
			}
			if (len(r.fields) > 0) != (mode == "logging") || (r.count == 1) != (mode == "metrics") || (r.ended == 1) != (mode == "tracing") {
				t.Fatalf("signal factory enabled another signal: fields=%v count=%v ended=%d", r.fields, r.count, r.ended)
			}
			info, _ := metadata.FromContext(ctx)
			if info.Header.Get("x-trace") != "original" || parent.ended != 0 {
				t.Fatal("native observation modified caller metadata or parent span")
			}
		})
	}
}

type forbiddenPropagation struct{}

func (forbiddenPropagation) Extract(context.Context, tracing.Carrier) context.Context {
	panic("disabled tracer extracted context")
}
func (forbiddenPropagation) Inject(context.Context, tracing.Carrier) {
	panic("disabled tracer injected context")
}

func TestNilNativeTracingPreservesEveryCallerBoundary(t *testing.T) {
	parent := &eventRecording{attrs: map[string]any{}}
	ctx := tracing.ContextWithSpan(t.Context(), parent)
	check := func(got context.Context) {
		t.Helper()
		if got != ctx || tracing.SpanFromContext(got) != parent {
			t.Fatal("disabled native tracing replaced context or parent span")
		}
	}
	cause := errors.New("unchanged")
	invoker := clientmw.AttemptTracing(nil, forbiddenPropagation{})(transport.InvokerFunc(func(got context.Context, _ string, _ any) (any, error) { check(got); return 7, cause }))
	if result, err := invoker.Invoke(ctx, "node", nil); result != 7 || err != cause {
		t.Fatal(result, err)
	}
	msg := &messaging.Message{Topic: "orders"}
	publish := messagingmw.PublishTracing(nil, forbiddenPropagation{})(func(got context.Context, input *messaging.Message) error {
		check(got)
		if input != msg {
			t.Fatal("disabled tracing replaced publish input")
		}
		return cause
	})
	if err := publish(ctx, msg); err != cause {
		t.Fatal(err)
	}
	consume := messagingmw.ConsumeTracing(nil, forbiddenPropagation{})(func(got context.Context, input *messaging.Message) error {
		check(got)
		if input != msg {
			t.Fatal("consume input changed")
		}
		return cause
	})
	if err := consume(ctx, msg); err != cause {
		t.Fatal(err)
	}
	task := &job.Job{Type: "refresh"}
	enqueue := jobmw.EnqueueTracing(nil, forbiddenPropagation{})(func(got context.Context, input *job.Job, options ...job.EnqueueOption) (string, error) {
		check(got)
		if input != task || job.ApplyOptions(options...).MaxRetry != 7 {
			t.Fatal("enqueue input/options changed")
		}
		return "job", cause
	})
	if id, err := enqueue(ctx, task, job.WithMaxRetry(7)); id != "job" || err != cause {
		t.Fatal(id, err)
	}
	execute := jobmw.ExecutionTracing(nil, forbiddenPropagation{})(func(got context.Context, input *job.Job) error {
		check(got)
		if input != task {
			t.Fatal("execute input changed")
		}
		return cause
	})
	if err := execute(ctx, task); err != cause {
		t.Fatal(err)
	}
	if parent.ended != 0 {
		t.Fatal("disabled tracing ended the parent span")
	}
}

type bucketRecording struct {
	*eventRecording
	buckets []float64
}

func (r *bucketRecording) Histogram(name, help string, buckets []float64, labels ...string) metricapi.Histogram {
	r.buckets = slices.Clone(buckets)
	return r.Meter.Histogram(name, help, buckets, labels...)
}

func TestNativeFactoriesHonorSignalOptions(t *testing.T) {
	r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
	reject := func(context.Context, *job.Job) error { return errorx.New(errorx.PermissionDenied, "DENIED", "private") }
	call := jobmw.ExecutionLogging(r, endpointmw.WithLogLevel(func(errorx.Code) logger.Level { return logger.LevelDebug }))(reject)
	if err := call(t.Context(), &job.Job{Type: "refresh"}); err == nil || r.level != logger.LevelDebug {
		t.Fatalf("logging option ignored: error=%v level=%v", err, r.level)
	}
	m := &bucketRecording{eventRecording: r}
	buckets := []float64{.1, .5}
	option := endpointmw.WithMetricBuckets(buckets)
	measured := jobmw.ExecutionMetrics(m, option)(reject)
	buckets[0] = 99
	if err := measured(t.Context(), &job.Job{Type: "refresh"}); err == nil || !slices.Equal(m.buckets, []float64{.1, .5}) {
		t.Fatalf("metrics option ignored: error=%v buckets=%v", err, m.buckets)
	}
}

func TestExplicitNativeSignalsObserveAndPreserveInterruption(t *testing.T) {
	marker := errors.New("private panic")
	for _, tt := range []struct {
		name      string
		abort     func()
		wantPanic any
	}{
		{name: "panic", abort: func() { panic(marker) }, wantPanic: marker},
		{name: "goexit", abort: runtime.Goexit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
			run := job.ChainExecution(jobmw.ExecutionTracing(r, nil), jobmw.ExecutionLogging(r), jobmw.ExecutionMetrics(r))(func(context.Context, *job.Job) error { tt.abort(); return nil })
			returned := false
			var recovered any
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				_ = run(t.Context(), &job.Job{Type: "refresh"})
				returned = true
			}()
			<-done
			if returned || recovered != tt.wantPanic {
				t.Fatalf("interruption changed: returned=%v panic=%v", returned, recovered)
			}
			if r.ended != 1 || r.count != 1 || r.attrs["servex.code"] != errorx.Internal.String() || errorx.Classify(r.failure).Reason != "INTERRUPTED" {
				t.Fatalf("interruption not observed: ended=%d count=%v attrs=%v failure=%v", r.ended, r.count, r.attrs, r.failure)
			}
		})
	}
}

type panickingPropagation struct{ marker any }

func (p panickingPropagation) Extract(ctx context.Context, _ tracing.Carrier) context.Context {
	return ctx
}
func (p panickingPropagation) Inject(context.Context, tracing.Carrier) { panic(p.marker) }
func TestNativeProducerEndsSpanWhenInjectionPanics(t *testing.T) {
	r := &eventRecording{Meter: noopmetrics.New(), attrs: map[string]any{}}
	marker := errors.New("private propagation panic")
	publish := messagingmw.PublishTracing(r, panickingPropagation{marker})(func(context.Context, *messaging.Message) error {
		t.Fatal("failed propagation reached publisher")
		return nil
	})
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = publish(t.Context(), &messaging.Message{Topic: "orders"})
	}()
	if recovered != marker || r.ended != 1 || errorx.Classify(r.failure).Reason != "INTERRUPTED" {
		t.Fatalf("injection panic changed or span leaked: panic=%v ended=%d failure=%v", recovered, r.ended, r.failure)
	}
}
