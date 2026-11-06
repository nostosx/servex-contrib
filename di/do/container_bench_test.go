package do_test

import (
	"context"
	"testing"

	"github.com/samber/do/v2"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/endpoint"
)

type benchmarkConfig struct{ value string }
type benchmarkRepository struct{ config *benchmarkConfig }
type benchmarkService struct{ repository *benchmarkRepository }

//go:noinline
func newBenchmarkConfig() *benchmarkConfig { return &benchmarkConfig{value: "hello"} }

//go:noinline
func newBenchmarkRepository(c *benchmarkConfig) *benchmarkRepository {
	return &benchmarkRepository{config: c}
}

//go:noinline
func newBenchmarkService(r *benchmarkRepository) *benchmarkService {
	return &benchmarkService{repository: r}
}

//go:noinline
func (s *benchmarkService) handle(context.Context, any) (any, error) {
	return s.repository.config, nil
}

func benchmarkManualGraph() endpoint.Endpoint {
	return newBenchmarkService(newBenchmarkRepository(newBenchmarkConfig())).handle
}

func benchmarkNativeGraph() do.Injector {
	i := do.New()
	do.Provide(i, func(do.Injector) (*benchmarkConfig, error) { return newBenchmarkConfig(), nil })
	do.Provide(i, func(i do.Injector) (*benchmarkRepository, error) {
		return newBenchmarkRepository(do.MustInvoke[*benchmarkConfig](i)), nil
	})
	do.Provide(i, func(i do.Injector) (*benchmarkService, error) {
		return newBenchmarkService(do.MustInvoke[*benchmarkRepository](i)), nil
	})
	do.Provide(i, func(i do.Injector) (endpoint.Endpoint, error) {
		return do.MustInvoke[*benchmarkService](i).handle, nil
	})
	return i
}

func BenchmarkResolve(b *testing.B) {
	b.Run("NativeDoCached", func(b *testing.B) {
		i := benchmarkNativeGraph()
		defer func() { _ = dox.Shutdown(context.Background(), i) }()
		do.MustInvoke[endpoint.Endpoint](i)
		for b.Loop() {
			if _, err := do.Invoke[endpoint.Endpoint](i); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// This measures complete assembly, first resolution and shutdown of a four-node
// in-memory graph. It excludes database/network work and includes lifecycle cost.
func BenchmarkAssemblyAndShutdown(b *testing.B) {
	b.Run("Manual", func(b *testing.B) {
		for b.Loop() {
			benchmarkManualGraph()
		}
	})
	b.Run("NativeDo", func(b *testing.B) {
		for b.Loop() {
			i := benchmarkNativeGraph()
			do.MustInvoke[endpoint.Endpoint](i)
			if err := dox.Shutdown(context.Background(), i); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkRequest(b *testing.B, mode string) endpoint.Endpoint {
	b.Helper()
	switch mode {
	case "Manual":
		return benchmarkManualGraph()
	case "NativePrewired", "NativeLookup":
		i := benchmarkNativeGraph()
		b.Cleanup(func() { _ = dox.Shutdown(context.Background(), i) })
		ep := do.MustInvoke[endpoint.Endpoint](i)
		if mode == "NativePrewired" {
			return ep
		}
		return func(ctx context.Context, req any) (any, error) {
			ep, err := do.Invoke[endpoint.Endpoint](i)
			if err != nil {
				return nil, err
			}
			return ep(ctx, req)
		}
	default:
		b.Fatalf("unknown mode %q", mode)
		return nil
	}
}

func BenchmarkRequest(b *testing.B) {
	for _, mode := range []string{"Manual", "NativePrewired", "NativeLookup"} {
		b.Run(mode, func(b *testing.B) {
			ep := benchmarkRequest(b, mode)
			ctx := context.Background()
			for b.Loop() {
				if _, err := ep(ctx, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRequestParallel(b *testing.B) {
	for _, mode := range []string{"Manual", "NativePrewired", "NativeLookup"} {
		b.Run(mode, func(b *testing.B) {
			ep := benchmarkRequest(b, mode)
			ctx := context.Background()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := ep(ctx, nil); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
