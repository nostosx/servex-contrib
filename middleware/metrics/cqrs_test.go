package metrics_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mwmetrics "github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex/cqrs"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

func TestTypedMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		meter := newRecordingMeter()
		buckets := []float64{.1, 1}
		mw := mwmetrics.Command[int, int](meter, nil, mwmetrics.WithBuckets(buckets))
		buckets[0] = 99
		ctx, cancel := context.WithCancel(metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "transport"}))
		cancel()
		failure := errorx.New(errorx.Canceled, "PRIVATE", "secret")
		base := cqrs.CommandFunc[int, int](func(got context.Context, req int) (int, error) {
			if got != ctx || got.Err() != context.Canceled {
				t.Error("context changed")
			}
			time.Sleep(200 * time.Millisecond)
			return req, failure
		})
		got, err := cqrs.ChainCommand("inventory.reserve", base, mw).Handle(ctx, 7)
		if got != 7 || err != failure {
			t.Fatal("result or error changed")
		}
		if meter.counters["servex_usecase_calls_total{command,inventory.reserve,Canceled}"] != 1 || !slices.Equal(meter.observations["servex_usecase_duration_seconds{command,inventory.reserve}"], []float64{.2}) {
			t.Fatalf("metrics: %v %v", meter.counters, meter.observations)
		}
		if len(meter.contexts) != 2 || meter.contexts[0] != ctx || meter.contexts[1] != ctx {
			t.Fatal("metrics did not receive the call context")
		}
		if !slices.Equal(meter.labelNames[mwmetrics.UsecaseCallsTotal], []string{"kind", "operation", "code"}) || !slices.Equal(meter.labelNames[mwmetrics.UsecaseDuration], []string{"kind", "operation"}) || !slices.Equal(meter.buckets[mwmetrics.UsecaseDuration], []float64{.1, 1}) {
			t.Fatal("declarations or bucket ownership changed")
		}
	})
}

func TestTypedMetricsPanic(t *testing.T) {
	t.Parallel()
	meter := newRecordingMeter()
	marker := &struct{}{}
	handler := cqrs.ChainQuery("catalog.get", cqrs.QueryFunc[int, int](func(context.Context, int) (int, error) { panic(marker) }), mwmetrics.Query[int, int](meter))
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic changed")
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	if meter.counters["servex_usecase_calls_total{query,catalog.get,Internal}"] != 1 || len(meter.observations["servex_usecase_duration_seconds{query,catalog.get}"]) != 1 {
		t.Fatalf("panic metrics: %v", meter.counters)
	}
}

func TestTypedMetricsConcurrent(t *testing.T) {
	t.Parallel()
	meter := newRecordingMeter()
	base := cqrs.QueryFunc[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := cqrs.ChainQuery("catalog.get", base, mwmetrics.Query[int, int](meter))
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
	if meter.counters["servex_usecase_calls_total{query,catalog.get,OK}"] != 32 || len(meter.observations["servex_usecase_duration_seconds{query,catalog.get}"]) != 32 {
		t.Fatal("lost concurrent observation")
	}
	if got, err := cqrs.ChainQuery("catalog.get", base, mwmetrics.Query[int, int](nil)).Handle(t.Context(), 3); got != 3 || err != nil {
		t.Fatal("nil meter changed call")
	}
}
