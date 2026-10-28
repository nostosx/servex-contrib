package grpcx

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport"
)

// StreamInfo is the request passed to streaming middleware. Method
// names and headers are available through metadata.FromContext. Message
// contents are deliberately not passed through the call-level chain.
type StreamInfo struct {
	ClientStreams bool
	ServerStreams bool
	sent          atomic.Int64
	received      atomic.Int64
}

// SentMessages reports successful sends on this side of the stream.
func (s *StreamInfo) SentMessages() int64 { return s.sent.Load() }

// ReceivedMessages reports successful receives on this side of the stream.
func (s *StreamInfo) ReceivedMessages() int64 { return s.received.Load() }

var errStreamReplayed = errorx.New(errorx.FailedPrecondition, "STREAM_REPLAYED", "grpcx: stream middleware must call next at most once")

// clientStreamCall owns the lifetime of one middleware execution. final is
// written only by runMiddlewareStream and read after settled has closed.
type clientStreamCall struct {
	opened  chan *clientStream
	settled chan struct{}
	final   error
}

// streamActivity joins message operations already in progress when gRPC
// reports completion. A client-streaming RPC can finish inside RecvMsg,
// before that method returns its one response and increments the counter.
type streamActivity struct {
	mu       sync.Mutex
	idle     *sync.Cond
	active   int
	finished bool
}

func newStreamActivity() *streamActivity {
	a := &streamActivity{}
	a.idle = sync.NewCond(&a.mu)
	return a
}

func (a *streamActivity) enter() func() {
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return func() {}
	}
	a.active++
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.active--
		if a.active == 0 {
			a.idle.Broadcast()
		}
		a.mu.Unlock()
	}
}

func (a *streamActivity) wait() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finished = true
	for a.active != 0 {
		a.idle.Wait()
	}
}

func (c *Client) newMiddlewareStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts []grpc.CallOption) (grpc.ClientStream, error) {
	if desc == nil {
		return nil, errorx.New(errorx.InvalidArgument, "STREAM_DESCRIPTION_MISSING", "grpcx: missing stream description")
	}
	caller := ctx
	ctx, cancel := context.WithCancel(ctx)
	call := &clientStreamCall{opened: make(chan *clientStream, 1), settled: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, errClientClosed
	}
	c.streams[call] = cancel
	c.streamWG.Add(1)
	c.mu.Unlock()
	go c.runMiddlewareStream(ctx, cancel, call, desc, method, opts)
	select {
	case stream := <-call.opened:
		return stream, nil
	case <-call.settled:
		// A server may finish immediately after creation. Preserve the
		// stream so the caller can read its messages and final status.
		select {
		case stream := <-call.opened:
			return stream, nil
		default:
			return nil, call.final
		}
	case <-caller.Done():
		return nil, fromStatus(caller, caller.Err())
	}
}

// runMiddlewareStream keeps synchronous endpoint middleware on the stack
// until gRPC's OnFinish callback reports the final status. OnFinish also runs
// for caller cancellation and Client.Close, even if the caller stops receiving.
func (c *Client) runMiddlewareStream(ctx context.Context, cancel context.CancelFunc, call *clientStreamCall, desc *grpc.StreamDesc, method string, opts []grpc.CallOption) {
	started := false
	defer c.streamWG.Done()
	defer func() {
		// The execution moved off the caller's stack. Contain a middleware
		// panic here too, so NewStream cannot hang when recovery was omitted.
		if recover() != nil {
			call.final = errorx.New(errorx.Internal, "STREAM_PANIC", "grpcx: stream middleware panicked")
		}
		if !started && call.final == nil {
			call.final = errorx.New(errorx.FailedPrecondition, "STREAM_NOT_STARTED", "grpcx: stream middleware returned without starting a stream")
		}
		cancel()
		close(call.settled)
		c.mu.Lock()
		delete(c.streams, call)
		c.mu.Unlock()
	}()
	var called atomic.Bool
	info := &StreamInfo{ClientStreams: desc.ClientStreams, ServerStreams: desc.ServerStreams}
	ep := c.tc.Endpoint(method, transport.InvokerFunc(func(ctx context.Context, addr string, _ any) (any, error) {
		if !called.CompareAndSwap(false, true) {
			return nil, errStreamReplayed
		}
		cc, release, err := c.acquire(addr)
		if err != nil {
			return nil, err
		}
		defer release()
		done := make(chan error, 1)
		activity := newStreamActivity()
		stream, err := cc.NewStream(outgoing(ctx), desc, method, append(slices.Clip(opts), grpc.OnFinish(func(err error) { done <- err }))...)
		if err != nil {
			return nil, fromStatus(ctx, err)
		}
		wrapped := &clientStream{
			ClientStream:  stream,
			ctx:           ctx,
			serverStreams: desc.ServerStreams,
			release:       func() {},
			result: func() error {
				<-call.settled
				return call.final
			},
			info:     info,
			activity: activity,
		}
		started = true
		call.opened <- wrapped
		err = <-done
		activity.wait()
		return wrapped, fromStatus(ctx, err)
	}), c.streamMiddleware...)
	_, call.final = ep(ctx, info)
}
