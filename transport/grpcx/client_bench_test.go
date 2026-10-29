package grpcx_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nostosx/servex-contrib/transport/grpcx"
	"github.com/nostosx/servex/endpoint"
)

type benchmarkEchoService struct {
	testpb.UnimplementedTestServiceServer
}

func (*benchmarkEchoService) UnaryCall(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Payload: req.GetPayload()}, nil
}

// Both clients reuse a warmed connection. Bufconn exercises gRPC serialization
// and dispatch without TCP; TCP uses a private ephemeral loopback listener.
func benchmarkGRPCCall(b *testing.B, network, mode string, size int) func() error {
	b.Helper()
	var ln net.Listener
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if network == "Bufconn" {
		listener := bufconn.Listen(1 << 20)
		ln = listener
		dialOptions = append(dialOptions, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	} else {
		var err error
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _ = ln.Close() })
	address := ln.Addr().String()
	if network == "Bufconn" {
		address = "127.0.0.1:19001"
	}
	svc := &benchmarkEchoService{}
	if mode == "Native" {
		s := grpc.NewServer()
		testpb.RegisterTestServiceServer(s, svc)
		go func() { _ = s.Serve(ln) }()
		b.Cleanup(s.Stop)
	} else {
		options := []grpcx.ServerOption{grpcx.WithListener(ln)}
		if mode == "ServexServer5Middleware" {
			mw := func(next endpoint.Endpoint) endpoint.Endpoint {
				return func(ctx context.Context, req any) (any, error) { return next(ctx, req) }
			}
			options = append(options, grpcx.WithMiddleware(mw, mw, mw, mw, mw))
		}
		s := grpcx.NewServer(options...)
		testpb.RegisterTestServiceServer(s, svc)
		if err := s.Start(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.Stop(ctx); err != nil {
				b.Error(err)
			}
		})
	}
	var conn grpc.ClientConnInterface
	if mode == "ServexBoth" {
		c, err := grpcx.NewClient(b.Context(), "grpc://"+address, grpcx.WithDialOptions(dialOptions...))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = c.Close() })
		conn = c.Conn()
	} else {
		c, err := grpc.NewClient("passthrough:///"+address, dialOptions...)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = c.Close() })
		conn = c
	}
	client := testpb.NewTestServiceClient(conn)
	req := &testpb.SimpleRequest{Payload: &testpb.Payload{Body: bytes.Repeat([]byte("x"), size)}}
	call := func(ctx context.Context) error {
		resp, err := client.UnaryCall(ctx, req)
		if err != nil {
			return err
		}
		if !bytes.Equal(resp.GetPayload().GetBody(), req.GetPayload().GetBody()) {
			return fmt.Errorf("gRPC echo response differs from request")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(b.Context(), 5*time.Second)
	defer cancel()
	if err := call(ctx); err != nil {
		b.Fatal(err)
	}
	return func() error { return call(b.Context()) }
}

func BenchmarkGRPCRoundTrip(b *testing.B) {
	for _, network := range []string{"Bufconn", "TCP"} {
		for _, size := range []int{128, 4096} {
			for _, mode := range []string{"Native", "ServexServer", "ServexBoth", "ServexServer5Middleware"} {
				b.Run(fmt.Sprintf("%s/%s/bytes=%d", network, mode, size), func(b *testing.B) {
					call := benchmarkGRPCCall(b, network, mode, size)
					for b.Loop() {
						if err := call(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkGRPCRoundTripParallel(b *testing.B) {
	for _, mode := range []string{"Native", "ServexServer", "ServexBoth"} {
		b.Run(mode, func(b *testing.B) {
			call := benchmarkGRPCCall(b, "TCP", mode, 128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := call(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
