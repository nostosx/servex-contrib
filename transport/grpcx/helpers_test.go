package grpcx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"

	"github.com/nostosx/servex-contrib/transport/grpcx"
)

// errUserNotFound is the business error returned by the test service.
var errUserNotFound = errorx.New(errorx.NotFound, "USER_NOT_FOUND", "user not found")

// network routes dial addresses to in-memory bufconn listeners.
type network struct {
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
	dials     atomic.Int32
	open      atomic.Int32 // dialed connections not yet closed
}

func newNetwork() *network {
	return &network{listeners: make(map[string]*bufconn.Listener)}
}

// listen returns a bufconn listener that reports addr as its address.
func (n *network) listen(addr string) net.Listener {
	lis := bufconn.Listen(1 << 20)
	n.mu.Lock()
	n.listeners[addr] = lis
	n.mu.Unlock()
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err)
	}
	return addrListener{Listener: lis, addr: tcp}
}

// dialOption returns the dial option that connects through the network.
func (n *network) dialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		n.dials.Add(1)
		n.mu.Lock()
		lis, ok := n.listeners[addr]
		n.mu.Unlock()
		if !ok {
			return nil, fmt.Errorf("no listener at %s", addr)
		}
		c, err := lis.DialContext(ctx)
		if err != nil {
			return nil, err
		}
		n.open.Add(1)
		return &trackedConn{Conn: c, n: n}, nil
	})
}

// trackedConn decrements the open connection count of its network on the
// first Close.
type trackedConn struct {
	net.Conn
	n    *network
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() { c.n.open.Add(-1) })
	return c.Conn.Close()
}

// addrListener is a bufconn listener with a TCP-looking address, so that
// URL and service registration see a host:port.
type addrListener struct {
	*bufconn.Listener
	addr net.Addr
}

func (l addrListener) Addr() net.Addr { return l.addr }

// testService implements grpc.testing.TestService. UnaryCall interprets the
// request payload body as a command.
type testService struct {
	testpb.UnimplementedTestServiceServer
	id      string
	blocked chan struct{} // receives when a "block" call starts

	mu    sync.Mutex
	infos []metadata.Info
}

func (s *testService) record(ctx context.Context) {
	info, _ := metadata.FromContext(ctx)
	s.mu.Lock()
	s.infos = append(s.infos, info)
	s.mu.Unlock()
}

// lastInfo returns the metadata.Info seen by the latest call.
func (s *testService) lastInfo() metadata.Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.infos) == 0 {
		return metadata.Info{}
	}
	return s.infos[len(s.infos)-1]
}

func (s *testService) UnaryCall(ctx context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	s.record(ctx)
	info, _ := metadata.FromContext(ctx)
	switch cmd := string(req.GetPayload().GetBody()); cmd {
	case "notfound":
		return nil, errUserNotFound.WithMetadata(map[string]string{"id": "404"})
	case "unavailable":
		return nil, errorx.New(errorx.Unavailable, "BUSY", "try later")
	case "status":
		return nil, status.Error(codes.PermissionDenied, "denied")
	case "plain":
		return nil, errors.New("boom")
	case "block":
		s.blocked <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	default:
		return &testpb.SimpleResponse{
			ServerId: s.id,
			Username: info.Header.Get("x-user"),
			Payload:  &testpb.Payload{Body: []byte(cmd)},
		}, nil
	}
}

func (s *testService) StreamingOutputCall(req *testpb.StreamingOutputCallRequest, stream grpc.ServerStreamingServer[testpb.StreamingOutputCallResponse]) error {
	s.record(stream.Context())
	info, _ := metadata.FromContext(stream.Context())
	for range req.GetResponseParameters() {
		if err := stream.Send(&testpb.StreamingOutputCallResponse{
			Payload: &testpb.Payload{Body: []byte(s.id + " " + info.Header.Get("x-user"))},
		}); err != nil {
			return err
		}
	}
	if st := req.GetResponseStatus(); st != nil {
		return errorx.New(errorx.Code(st.GetCode()), "STREAM_FAILED", st.GetMessage())
	}
	return nil
}

func (s *testService) StreamingInputCall(stream grpc.ClientStreamingServer[testpb.StreamingInputCallRequest, testpb.StreamingInputCallResponse]) error {
	size := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&testpb.StreamingInputCallResponse{AggregatedPayloadSize: int32(size)})
		}
		if err != nil {
			return err
		}
		size += len(req.GetPayload().GetBody())
	}
}

// startServer starts a grpcx.Server with the test service on addr of n.
func startServer(t *testing.T, n *network, addr string, opts ...grpcx.ServerOption) (*grpcx.Server, *testService) {
	t.Helper()
	svc := &testService{id: addr, blocked: make(chan struct{}, 1)}
	srv := grpcx.NewServer(append([]grpcx.ServerOption{grpcx.WithListener(n.listen(addr))}, opts...)...)
	testpb.RegisterTestServiceServer(srv, svc)
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return srv, svc
}

// roundRobin is a deterministic Selector rotating through the nodes.
type roundRobin struct {
	next atomic.Int64

	mu   sync.Mutex
	done []error
}

func (r *roundRobin) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	if len(nodes) == 0 {
		return selector.Node{}, nil, selector.ErrNoAvailable
	}
	i := int(r.next.Add(1) - 1)
	return nodes[i%len(nodes)], func(_ context.Context, err error) {
		r.mu.Lock()
		r.done = append(r.done, err)
		r.mu.Unlock()
	}, nil
}

func (r *roundRobin) outcomes() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.done...)
}

// simple builds a UnaryCall request with body cmd.
func simple(cmd string) *testpb.SimpleRequest {
	return &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte(cmd)}}
}
