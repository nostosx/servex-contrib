package grpcx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/metadata"
)

func TestUnaryMiddlewareComposedOnceWithPerCallHandler(t *testing.T) {
	var builds, calls atomic.Int32
	s := NewServer(WithMiddleware(func(next endpoint.Endpoint) endpoint.Endpoint {
		builds.Add(1)
		return func(ctx context.Context, req any) (any, error) {
			calls.Add(1)
			return next(ctx, req)
		}
	}))
	if got := builds.Load(); got != 1 {
		t.Fatalf("middleware built %d times during assembly", got)
	}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			id := fmt.Sprint(i)
			operation := "/benchmark.Service/Call" + id
			ctx := grpcmd.NewIncomingContext(t.Context(), grpcmd.Pairs("x-request-id", id))
			req := &struct{ ID string }{ID: id}
			resp, err := s.unaryInterceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: operation}, func(ctx context.Context, got any) (any, error) {
				info, ok := metadata.FromContext(ctx)
				if !ok || info.Operation != operation || info.Header.Get("x-request-id") != id || got != req {
					return nil, fmt.Errorf("request handler/context crossed calls: %+v", info)
				}
				return req, nil
			})
			if err != nil || resp != req {
				t.Errorf("call %s response=%v error=%v", id, resp, err)
			}
		})
	}
	wg.Wait()
	if builds.Load() != 1 || calls.Load() != 64 {
		t.Fatalf("middleware builds=%d calls=%d", builds.Load(), calls.Load())
	}
}
