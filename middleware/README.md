# middleware

Module version for this revision: **v1.6.0**.

Explicit AOP policies for published servex v1.7.0 contracts. Requires Go 1.27.3.
See the [validation commands](../CONTRIBUTING.md#checks).

## Package layout

Ordinary policies live in one package per execution boundary. Functions and files
name the capability; using logging, metrics and recovery together needs one import.

| Package | Execution boundary | Main factories |
|---|---|---|
| [endpoint](endpoint) | Inbound endpoints and server streams | Logging, Metrics, Tracing, Authentication, Validation, Recovery, RateLimit, ContextRateLimit, Budget, Bulkhead |
| [application](application) | Typed business handlers, commands and queries | Logging, Metrics, Tracing, Validation, Authorization, Transaction, Idempotency, Audit, QueryCache, Budget, Bulkhead |
| [client](client) | Outbound logical calls and selected-node attempts | Logging, Metrics, Tracing, AttemptLogging, AttemptMetrics, AttemptTracing, Authentication, CircuitBreaker, Bulkhead |
| [messaging](messaging) | Publication, consumption and native settlement events | PublishLogging/Metrics/Tracing, ConsumeLogging/Metrics/Tracing, SettlementLogging/Metrics/Tracing, Identity, Timeout, Recovery, Deduplicate, Bulkhead |
| [job](job) | Enqueue, execution and native runtime events | EnqueueLogging/Metrics/Tracing, ExecutionLogging/Metrics/Tracing, EventLogging/Metrics/Tracing, Timeout, Recovery, Deduplicate, Bulkhead |
| [bulkhead](bulkhead) | Explicitly shared concurrency capacity | New creates a Limiter; Acquire grants a release function |

Each boundary directly implements its execution wrappers. Shared signal recording
and option resolution live in [observe](observe), which also provides the default
`Level` classifier (use `errorx.IsServerFault` for fault classification);
[tokenauth](tokenauth) and [validation](validation) provide named support functions.
They belong to the same module and introduce no SDK dependency. There are no
internal directories or separate signal-factory forwarding packages. Cross-scope
observation tests and benchmarks live in the module root's test files.

Endpoint and Client logical-call factories return core `endpoint.Middleware`.
Application factories return `application.Middleware[Request, Result]`. Native
attempt, message and job factories retain their own contracts. No implicit
policies are installed and the first wrapper is outermost. `endpoint.Recommended`
and `client.Recommended` return a chain in canonical order, and `Order`/`Check`
expose and verify that order for teams that assemble chains by hand; see
[Policy order](#policy-order).

The middleware module is SDK-free. Optional SDK adapters are independent
capability modules: [auth](../auth), [validation](../validation) and
[ratelimit](../ratelimit); browser sessions and login live under
[session](../session) and [auth/login](../auth/login). Concrete signal backends
live under [observability](../observability).

## Endpoint middleware

```go
import endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"

handler := endpoint.Chain(
    endpointmw.Tracing(tracer, propagator),
    endpointmw.Logging(log),
    endpointmw.Metrics(meter),
    endpointmw.Recovery(endpointmw.WithRecoveryLogger(log)),
    endpointmw.Authentication(verify),
    endpointmw.Validation(),
)(businessEndpoint)
```

Pass the same factories to httpx.WithMiddleware or grpcx.WithMiddleware. A matched
endpoint chain does not cover raw handlers or routing failures. For a whole HTTP
request use httpx.WithHTTPMiddleware and httpx.EndpointFilter. Native HTTP
middleware handles CORS, CSRF, login and Session.

Authentication exposes the verified principal through PrincipalFromContext.
Client Authentication fails closed: an empty token returns Unauthenticated unless
WithAllowAnonymous is supplied explicitly.
RateLimit accepts a synchronous limiter; ContextRateLimit accepts a backend using
context. Recovery returns non-retryable Internal/PANIC; raw panic diagnostics
require WithPanicDetails. Budget sets a cooperative execution deadline. Validation
calls the request's Validate method. Optional SDK validation, JWT/OIDC and rate
limit adapters remain separately selected dependencies.

## Typed application middleware

```go
import applicationmw "github.com/nostosx/servex-contrib/middleware/application"

handler := application.Decorate(application.Metadata{
    Kind: application.KindCommand, Operation: "run.start",
}, startRunHandler,
    applicationmw.Tracing[StartRun, Run](tracer),
    applicationmw.Logging[StartRun, Run](log),
    applicationmw.Metrics[StartRun, Run](meter),
    applicationmw.Authorization[StartRun, Run](authorizeStartRun),
    applicationmw.Idempotency(idempotencyConfig),
    applicationmw.Transaction[StartRun, Run](transactionRunner),
)
```

These are assembly fragments: application-owned types, handlers, sinks and policies
are supplied by app.go. IdempotencyConfig and QueryCacheConfig carry typed policy
settings; TransactionRunner/TransactionRunnerFunc define the transaction boundary.
Audit records business outcomes. Validation accepts a typed check, with a nil check
selecting the request's Validate method. QueryCache applies only to queries, with
explicit TTL, trusted scope and FailOpen behavior.

application.Wrap retains a function signature. application.Decorate with
`Metadata{Kind: application.KindCommand}` or `KindQuery` supplies intent metadata
for the same handler contract. Conditional composition uses
application.When and application.Operations. Direct receiver calls bypass decorators.
Recovery, retries and transactions are explicit. Observation records interrupted
Internal outcomes while panic/Goexit continue unchanged. Requests, results, raw
error text and panic values are excluded from observation signals.

A PostgreSQL idempotency store can commit successful results with participating
business SQL writes. Audit atomicity needs a recorder joining that transaction;
resource authorization needing its snapshot belongs inside the transaction.
Cache invalidation, consistency and stampede handling remain application choices.

## Client middleware

```go
import clientmw "github.com/nostosx/servex-contrib/middleware/client"

remote, err := httpx.NewClient(ctx, target,
    httpx.WithClientMiddleware(
        clientmw.Tracing(tracer, propagator),
        clientmw.Logging(log),
        clientmw.Metrics(meter),
        clientmw.Authentication(token),
        clientmw.CircuitBreaker(),
    ),
    httpx.WithClientAttemptMiddleware(
        clientmw.AttemptTracing(tracer, propagator),
        clientmw.AttemptLogging(log),
        clientmw.AttemptMetrics(meter),
    ),
    httpx.WithCallPolicy(clientpolicy.Policy{
        Timeout: 3*time.Second, AttemptTimeout: time.Second,
        MaxAttempts: 3, RetrySafe: true, RetryIf: errorx.IsRetryable,
    }),
)
```

clientpolicy names contrib transport/client; grpcx exposes the same options.
A logical-call chain wraps all attempts and backoff. An attempt chain wraps one
selected-node invocation and never replays next. transport/client.Attempt adapts
an explicitly chosen endpoint-shaped policy to one attempt; node-specific policies
also need a node-aware key. Client Authentication injects a token; inbound token
verification belongs to endpoint.Authentication.

Retries and total/attempt deadlines belong exclusively to transport/client.Policy.
WithOperationPolicies replaces the policy for exact stable operation names.
Multiple attempts require RetrySafe and RetryIf; Backoff may use kitx/backoff.
Configure SDK retry behavior to keep one retry owner. Logging uses WithLogLevel;
metrics uses WithMetricBuckets. Only tracing owns trace propagation.

## Message and job middleware

```go
import messagingmw "github.com/nostosx/servex-contrib/middleware/messaging"

publisher := messaging.DecoratePublisher(rawPublisher,
    messagingmw.Identity(),
    messagingmw.PublishTracing(tracer, propagator),
    messagingmw.PublishLogging(log),
    messagingmw.PublishMetrics(meter),
)
consume := messaging.ChainConsume(
    messagingmw.ConsumeTracing(tracer, propagator),
    messagingmw.ConsumeLogging(log),
    messagingmw.ConsumeMetrics(meter),
    messagingmw.Recovery(),
    messagingmw.Timeout(2*time.Second),
    messagingmw.Deduplicate(store, messageKey),
)(handleMessage)
```

messaging and job name core contracts; messagingmw and jobmw identify contrib
policies. Job composition uses job.ChainEnqueue and job.ChainExecution with
jobmw.EnqueueLogging/Metrics/Tracing and jobmw.ExecutionLogging/Metrics/Tracing.
Each boundary preserves native inputs and options.

```go
import jobmw "github.com/nostosx/servex-contrib/middleware/job"

execute := job.ChainExecution(
    jobmw.ExecutionTracing(tracer, propagator),
    jobmw.ExecutionLogging(log),
    jobmw.ExecutionMetrics(meter),
    jobmw.Recovery(),
    jobmw.Timeout(2*time.Second),
)(handleJob)
```

Message ACK/redelivery and job scheduling, leases, retries and completion remain
with the backend. Deduplicate requires a stable ID, trusted scope, canonical digest
and Store; it does not make arbitrary external effects or acknowledgement atomic.
Use messagingmw.SettlementLogging/Metrics/Tracing and jobmw.EventLogging/Metrics/Tracing
to observe real native outcomes. Combine selected callbacks explicitly and pass
them to backend observer options; handler success alone is not settlement success.

## Streams and limits

Select grpcx.WithStreamMiddleware and WithClientStreamMiddleware separately from
unary chains, using endpoint and client factories respectively. They wrap the
whole stream lifetime without replay or implicit per-message validation. SSE,
WebSocket and Connect may use whole-request HTTP policies; message processing
remains explicit.

Budget and Timeout use cooperative context deadlines. They do not detach or
forcibly terminate handlers. Create one middleware/bulkhead.Limiter and pass it
to the boundary Bulkhead factories only when those chains should share capacity.
Use different limiters for different isolation scopes.

## Policy order

The first wrapper is outermost, so order changes meaning. `endpoint.Recommended`
and `client.Recommended` take a `map[Policy]endpoint.Middleware` and return the
chain in canonical order, omitting nil entries; `Order` returns that sequence and
`Check` verifies a hand-built one. An unknown policy panics during assembly.

Inbound, outermost first:

| Policy | Position rationale |
|---|---|
| `PolicyTracing` | Outermost, so rejected calls are traced too |
| `PolicyLogging` | Inside tracing for trace/span IDs, outside recovery so a recovered panic is logged as a classified outcome |
| `PolicyMetrics` | Same reasoning as logging |
| `PolicyRecovery` | Outside every remaining policy, so a panic anywhere downstream becomes a non-retryable Internal error |
| `PolicyBudget` | Bounds all downstream work including remote admission and verification |
| `PolicyRateLimit` | Rejects over-quota calls before a concurrency slot is spent |
| `PolicyBulkhead` | Bounds concurrency before paying for authentication |
| `PolicyAuthentication` | Establishes the principal |
| `PolicyValidation` | Closest to the handler |

Outbound, outermost first:

| Policy | Position rationale |
|---|---|
| `PolicyTracing` | Outermost, so the span covers attempts and local rejections |
| `PolicyLogging` / `PolicyMetrics` | Inside tracing; they observe accepted and rejected calls |
| `PolicyAuthentication` | Outside the breaker, so an unavailable token source never counts against the target's health |
| `PolicyBulkhead` | Must stay outside the breaker: `IsCircuitBreakerFailure` counts `ResourceExhausted` as a backend failure, so an inner bulkhead would let local overload open it |
| `PolicyCircuitBreaker` | Innermost logical-call policy, so local authentication and bulkhead rejections do not count against the backend |

```go
handler := endpoint.Chain(endpointmw.Recommended(map[endpointmw.Policy]endpoint.Middleware{
	endpointmw.PolicyTracing:        endpointmw.Tracing(tracer, propagator),
	endpointmw.PolicyLogging:        endpointmw.Logging(log),
	endpointmw.PolicyRecovery:       endpointmw.Recovery(endpointmw.WithRecoveryLogger(log)),
	endpointmw.PolicyAuthentication: endpointmw.Authentication(verify),
	endpointmw.PolicyValidation:     endpointmw.Validation(),
})...)(businessEndpoint)
```

## Validation

From the repository root:

```sh
task check lint vulncheck MODULES=middleware
```

Select independent SDK modules explicitly when changing them. Tests cover ordering,
error identity, safe classification, propagation, interruption, business policy
composition and native runtime events. The observe package shares recording
semantics; each execution boundary owns its middleware factories.
