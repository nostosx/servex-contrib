package middleware_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
)

type request struct{ ID string }

//go:noinline
func (r *request) Validate() error {
	if r.ID == "" {
		return errors.New("missing request ID")
	}
	return nil
}

// discardLogger isolates middleware attribute construction from log encoding
// and output. Keep the SDK-free logger call boundary observable to the compiler.
type discardLogger struct{ enabled bool }

func (l *discardLogger) Enabled(context.Context, logger.Level) bool { return l.enabled }

//go:noinline
func (*discardLogger) Log(_ context.Context, _ logger.Level, _ string, attrs ...any) {
	runtime.KeepAlive(attrs)
}

//go:noinline
func handle(_ context.Context, req any) (any, error) { return req, nil }

// staticVerify deliberately excludes token cryptography, network requests and
// principal lookup. It measures only the middleware's token and context work.
func staticVerify(_ context.Context, token string) (string, error) {
	if token != "benchmark-token" {
		return "", errors.New("unexpected benchmark token")
	}
	return "principal-a", nil
}

func localLimit() endpoint.Middleware {
	// This benchmark measures successful admission with one retained key.
	// The rate and burst prevent quota exhaustion under concurrent execution.
	return endpointmw.RateLimit(endpointmw.TokenBucket(1e12, 1_000_000_000))
}

func setup(b *testing.B, mode string) (endpoint.Endpoint, context.Context, *request) {
	b.Helper()
	var chain []endpoint.Middleware
	kind := metadata.KindServer
	switch mode {
	case "Direct":
	case "Recovery":
		chain = []endpoint.Middleware{endpointmw.Recovery()}
	case "Validate":
		chain = []endpoint.Middleware{endpointmw.Validation()}
	case "AuthStaticVerify":
		chain = []endpoint.Middleware{endpointmw.Authentication(staticVerify)}
	case "LoggingNil":
		chain = []endpoint.Middleware{endpointmw.Logging(nil)}
	case "LoggingDisabled":
		chain = []endpoint.Middleware{endpointmw.Logging(&discardLogger{})}
	case "LoggingDiscard":
		chain = []endpoint.Middleware{endpointmw.Logging(&discardLogger{enabled: true})}
	case "MetricsNoop":
		chain = []endpoint.Middleware{endpointmw.Metrics(nil)}
	case "TracingNoop":
		chain = []endpoint.Middleware{endpointmw.Tracing(nil, nil)}
	case "ServerBudgetSuccess":
		chain = []endpoint.Middleware{endpointmw.Budget(time.Second)}
	case "RateLimitSingleKey":
		chain = []endpoint.Middleware{localLimit()}
	case "CircuitBreakerSuccess":
		kind = metadata.KindClient
		chain = []endpoint.Middleware{client.CircuitBreaker()}
	case "ServerChain":
		chain = []endpoint.Middleware{
			endpointmw.Recovery(), endpointmw.Authentication(staticVerify), endpointmw.Validation(),
			endpointmw.Logging(&discardLogger{enabled: true}), endpointmw.Metrics(nil),
			endpointmw.Tracing(nil, nil), localLimit(),
		}
	default:
		b.Fatalf("unknown middleware mode %q", mode)
	}
	// All metadata and assembly work is outside the timed loop. Only server
	// middleware reads Header; no measured chain mutates this shared map.
	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind: kind, Protocol: "http", Operation: "POST /v1/benchmark",
		Header: metadata.Pairs("authorization", "Bearer benchmark-token"),
	})
	req := &request{ID: "request-a"}
	ep := endpoint.Chain(chain...)(handle)
	if resp, err := ep(ctx, req); err != nil || resp != req {
		b.Fatalf("warmup result = %v, %v", resp, err)
	}
	return ep, ctx, req
}

func BenchmarkMiddleware(b *testing.B) {
	for _, mode := range []string{
		"Direct", "Recovery", "Validate", "AuthStaticVerify", "LoggingNil",
		"LoggingDisabled", "LoggingDiscard", "MetricsNoop", "TracingNoop",
		"ServerBudgetSuccess", "RateLimitSingleKey", "CircuitBreakerSuccess",
		"ServerChain",
	} {
		b.Run(mode, func(b *testing.B) {
			ep, ctx, req := setup(b, mode)
			for b.Loop() {
				if resp, err := ep(ctx, req); err != nil || resp != req {
					b.Fatalf("result = %v, %v", resp, err)
				}
			}
		})
	}
}

func BenchmarkMiddlewareParallel(b *testing.B) {
	for _, mode := range []string{
		"Direct", "RateLimitSingleKey", "CircuitBreakerSuccess", "ServerChain",
	} {
		b.Run(mode, func(b *testing.B) {
			ep, ctx, req := setup(b, mode)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if resp, err := ep(ctx, req); err != nil || resp != req {
						b.Errorf("result = %v, %v", resp, err)
						return
					}
				}
			})
		})
	}
}
