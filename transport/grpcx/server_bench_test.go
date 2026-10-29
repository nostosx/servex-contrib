package grpcx

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	testpb "google.golang.org/grpc/interop/grpc_testing"

	"github.com/nostosx/servex/endpoint"
)

func BenchmarkUnaryInterceptor(b *testing.B) {
	for _, count := range []int{-1, 0, 1, 5} {
		name := "NativeHandler"
		if count >= 0 {
			name = fmt.Sprintf("Servex/middleware=%d", count)
		}
		b.Run(name, func(b *testing.B) {
			handler := grpc.UnaryHandler(func(_ context.Context, req any) (any, error) { return req, nil })
			ctx, req := context.Background(), &testpb.Empty{}
			call := handler
			if count >= 0 {
				middleware := make([]endpoint.Middleware, count)
				for idx := range middleware {
					middleware[idx] = benchmarkPassthrough
				}
				s := NewServer(WithMiddleware(middleware...))
				info := &grpc.UnaryServerInfo{FullMethod: "/benchmark.Echo/Call"}
				call = func(ctx context.Context, req any) (any, error) {
					return s.unaryInterceptor(ctx, req, info, handler)
				}
			}
			for b.Loop() {
				resp, err := call(ctx, req)
				if err != nil || resp != req {
					b.Fatalf("interceptor response=%v error=%v", resp, err)
				}
			}
		})
	}
}

//go:noinline
func benchmarkPassthrough(next endpoint.Endpoint) endpoint.Endpoint {
	return func(ctx context.Context, req any) (any, error) { return next(ctx, req) }
}
