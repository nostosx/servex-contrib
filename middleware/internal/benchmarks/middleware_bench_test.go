// Package benchmarks measures middleware success paths without external SDKs.
package benchmarks

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/middleware/auth"
	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex-contrib/middleware/logging"
	"github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex-contrib/middleware/ratelimit"
	"github.com/nostosx/servex-contrib/middleware/recovery"
	"github.com/nostosx/servex-contrib/middleware/retry"
	"github.com/nostosx/servex-contrib/middleware/timeout"
	"github.com/nostosx/servex-contrib/middleware/tracing"
	"github.com/nostosx/servex-contrib/middleware/validate"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/logger"
	"github.com/nostosx/servex/metadata"
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
	return ratelimit.New(ratelimit.TokenBucket(1e12, 1_000_000_000))
}

func setup(b *testing.B, mode string) (endpoint.Endpoint, context.Context, *request) {
	b.Helper()
	var chain []endpoint.Middleware
	kind := metadata.KindServer
	switch mode {
	case "Direct":
	case "Recovery":
		chain = []endpoint.Middleware{recovery.New()}
	case "Validate":
		chain = []endpoint.Middleware{validate.New()}
	case "AuthStaticVerify":
		chain = []endpoint.Middleware{auth.New(staticVerify)}
	case "LoggingNil":
		chain = []endpoint.Middleware{logging.New(nil)}
	case "LoggingDisabled":
		chain = []endpoint.Middleware{logging.New(&discardLogger{})}
	case "LoggingDiscard":
		chain = []endpoint.Middleware{logging.New(&discardLogger{enabled: true})}
	case "MetricsNoop":
		chain = []endpoint.Middleware{metrics.New(nil)}
	case "TracingNoop":
		chain = []endpoint.Middleware{tracing.New(nil, nil)}
	case "TimeoutSuccess":
		kind = metadata.KindClient
		chain = []endpoint.Middleware{timeout.New(time.Second)}
	case "RateLimitSingleKey":
		chain = []endpoint.Middleware{localLimit()}
	case "CircuitBreakerSuccess":
		kind = metadata.KindClient
		chain = []endpoint.Middleware{circuitbreaker.New()}
	case "RetrySuccess":
		kind = metadata.KindClient
		chain = []endpoint.Middleware{retry.New()}
	case "ServerChain":
		chain = []endpoint.Middleware{
			recovery.New(), auth.New(staticVerify), validate.New(),
			logging.New(&discardLogger{enabled: true}), metrics.New(nil),
			tracing.New(nil, nil), localLimit(),
		}
	case "ClientChain":
		kind = metadata.KindClient
		chain = []endpoint.Middleware{timeout.New(time.Second), retry.New(), circuitbreaker.New()}
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
		"TimeoutSuccess", "RateLimitSingleKey", "CircuitBreakerSuccess", "RetrySuccess",
		"ServerChain", "ClientChain",
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
		"Direct", "RateLimitSingleKey", "CircuitBreakerSuccess", "ServerChain", "ClientChain",
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
