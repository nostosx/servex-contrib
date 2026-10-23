package grpcx

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/health"
)

// healthServer implements grpc.health.v1.Health with every known service
// following a single health.Checker.
type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer

	s        *Server
	checker  health.Checker
	interval time.Duration
}

// known reports whether service is the empty server-wide name or a service
// registered on the server.
func (h *healthServer) known(service string) bool {
	if service == "" {
		return true
	}
	_, ok := h.s.srv.GetServiceInfo()[service]
	return ok
}

// status evaluates the checker for a known service.
func (h *healthServer) status(ctx context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
	select {
	case <-h.s.stopping:
		return grpc_health_v1.HealthCheckResponse_NOT_SERVING
	default:
	}
	if err := h.checker.Check(ctx); err != nil {
		return grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}
	return grpc_health_v1.HealthCheckResponse_SERVING
}

// Check reports the current status of req.Service.
func (h *healthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if !h.known(req.GetService()) {
		return nil, status.Errorf(codes.NotFound, "grpcx: unknown service %q", req.GetService())
	}
	return &grpc_health_v1.HealthCheckResponse{Status: h.status(ctx)}, nil
}

// List reports the status of the server and of every registered service.
func (h *healthServer) List(ctx context.Context, _ *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	st := h.status(ctx)
	services := h.s.srv.GetServiceInfo()
	statuses := make(map[string]*grpc_health_v1.HealthCheckResponse, len(services)+1)
	statuses[""] = &grpc_health_v1.HealthCheckResponse{Status: st}
	for name := range services {
		statuses[name] = &grpc_health_v1.HealthCheckResponse{Status: st}
	}
	return &grpc_health_v1.HealthListResponse{Statuses: statuses}, nil
}

// Watch sends the status of req.Service immediately and again whenever it
// changes, re-evaluating the checker periodically. When the server stops,
// Watch sends NOT_SERVING and ends the stream with Unavailable so that
// graceful shutdown is not held up.
func (h *healthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse]) error {
	ctx := stream.Context()
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	last := grpc_health_v1.HealthCheckResponse_ServingStatus(-1)
	for {
		st := grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
		if h.known(req.GetService()) {
			st = h.status(ctx)
		}
		if st != last {
			if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: st}); err != nil {
				return err
			}
			last = st
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-h.s.stopping:
			if last != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
				_ = stream.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING})
			}
			return status.Error(codes.Unavailable, "grpcx: server stopping")
		case <-ticker.C:
		}
	}
}
