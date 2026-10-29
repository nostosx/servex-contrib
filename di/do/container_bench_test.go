package do_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/samber/do/v2"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/di"
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

func benchmarkServexGraph(tb testing.TB) *dox.Container {
	c := dox.New()
	for _, err := range []error{
		di.Provide(c, func(di.Resolver) (*benchmarkConfig, error) { return newBenchmarkConfig(), nil }),
		di.Provide(c, func(r di.Resolver) (*benchmarkRepository, error) {
			return newBenchmarkRepository(di.MustResolve[*benchmarkConfig](r)), nil
		}),
		di.Provide(c, func(r di.Resolver) (*benchmarkService, error) {
			return newBenchmarkService(di.MustResolve[*benchmarkRepository](r)), nil
		}),
		di.Provide(c, func(r di.Resolver) (endpoint.Endpoint, error) {
			return di.MustResolve[*benchmarkService](r).handle, nil
		}),
	} {
		if err != nil {
			tb.Fatal(err)
		}
	}
	return c
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

func benchmarkResolve[T any](tb testing.TB, r di.Resolver) T {
	v, err := di.Resolve[T](r)
	if err != nil {
		tb.Fatal(err)
	}
	return v
}

// The resolver stub isolates core's typed helper from a real container's
// synchronization and lookup policy. It deliberately performs no lookup.
type benchmarkStaticResolver struct{ value any }

//go:noinline
func (r *benchmarkStaticResolver) Resolve(reflect.Type) (any, error) { return r.value, nil }

func BenchmarkResolve(b *testing.B) {
	b.Run("CoreHelperStaticResolver", func(b *testing.B) {
		var r di.Resolver = &benchmarkStaticResolver{value: newBenchmarkConfig()}
		for b.Loop() {
			if _, err := di.Resolve[*benchmarkConfig](r); err != nil {
				b.Fatal(err)
			}
		}
	})
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
	b.Run("ServexDoCached", func(b *testing.B) {
		c := benchmarkServexGraph(b)
		defer func() { _ = c.Shutdown(context.Background()) }()
		benchmarkResolve[endpoint.Endpoint](b, c)
		for b.Loop() {
			if _, err := di.Resolve[endpoint.Endpoint](c); err != nil {
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
	b.Run("ServexDo", func(b *testing.B) {
		for b.Loop() {
			c := benchmarkServexGraph(b)
			benchmarkResolve[endpoint.Endpoint](b, c)
			if err := c.Shutdown(context.Background()); err != nil {
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
	case "ServexPrewired", "ServexLookup":
		c := benchmarkServexGraph(b)
		b.Cleanup(func() { _ = c.Shutdown(context.Background()) })
		ep := benchmarkResolve[endpoint.Endpoint](b, c)
		if mode == "ServexPrewired" {
			return ep
		}
		return func(ctx context.Context, req any) (any, error) {
			ep, err := di.Resolve[endpoint.Endpoint](c)
			if err != nil {
				return nil, err
			}
			return ep(ctx, req)
		}
	case "NativeLookup":
		i := benchmarkNativeGraph()
		b.Cleanup(func() { _ = dox.Shutdown(context.Background(), i) })
		do.MustInvoke[endpoint.Endpoint](i)
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
	for _, mode := range []string{"Manual", "ServexPrewired", "NativeLookup", "ServexLookup"} {
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
	for _, mode := range []string{"Manual", "ServexPrewired", "NativeLookup", "ServexLookup"} {
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
