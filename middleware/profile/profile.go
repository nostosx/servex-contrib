// Package profile assembles SDK-free observability middleware for transport
// scopes. Authentication and operation policies remain explicit additions.
package profile

import (
	"slices"

	"github.com/nostosx/servex-contrib/middleware/logging"
	"github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex-contrib/middleware/recovery"
	"github.com/nostosx/servex-contrib/middleware/tracing"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/logger"
	metricapi "github.com/nostosx/servex/observability/metrics"
	traceapi "github.com/nostosx/servex/observability/tracing"
)

// Config provides the same explicitly selected signals to every protocol.
// Implementations must support concurrent use. Nil signals retain the individual
// middleware's behavior: logging disabled, no-op metrics and tracing.
type Config struct {
	Logger     logger.Logger
	Tracer     traceapi.Tracer
	Propagator traceapi.Propagator
	Meter      metricapi.Meter
}

// Profile owns immutable middleware chains. Construct once in app.go and use
// independent snapshots for HTTP endpoints, gRPC unary calls and whole streams.
// Transport metadata determines server/client span kind and metric labels.
type Profile struct {
	server []endpoint.Middleware
	client []endpoint.Middleware
}

// New creates middleware without starting work or selecting an SDK backend.
// Metrics are registered once and shared by the returned chains.
func New(config Config) *Profile {
	trace := tracing.New(config.Tracer, config.Propagator)
	log := logging.New(config.Logger)
	measure := metrics.New(config.Meter)
	return &Profile{
		server: []endpoint.Middleware{trace, log, measure, recovery.New(recovery.WithLogger(config.Logger))},
		client: []endpoint.Middleware{trace, log, measure},
	}
}

// Server returns tracing → logging → metrics → recovery → extra. The first
// middleware is outermost. Recovery turns business panics into errors before
// the surrounding observability middleware records the completed invocation.
func (p *Profile) Server(extra ...endpoint.Middleware) []endpoint.Middleware {
	return combine(p.server, extra)
}

// Stream observes one whole stream lifetime, with the same outer-to-inner order
// as Server. No retry, timeout or per-message validation is installed. Explicit
// extras must be safe for whole streams; unary policies are not inferred.
func (p *Profile) Stream(extra ...endpoint.Middleware) []endpoint.Middleware {
	return combine(p.server, extra)
}

// Client returns tracing → logging → metrics → extra. It adds no recovery,
// retries, timeouts or replay policies to the caller's operation.
func (p *Profile) Client(extra ...endpoint.Middleware) []endpoint.Middleware {
	return combine(p.client, extra)
}

func combine(base, extra []endpoint.Middleware) []endpoint.Middleware {
	chain := make([]endpoint.Middleware, 0, len(base)+len(extra))
	chain = append(chain, base...)
	chain = append(chain, extra...)
	return slices.DeleteFunc(chain, func(middleware endpoint.Middleware) bool { return middleware == nil })
}
