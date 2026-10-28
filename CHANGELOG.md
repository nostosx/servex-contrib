# Changelog

Modules are versioned independently with tags of the form `<module>/vX.Y.Z`.
The initial release of each module is v1.0.0. Patch releases preserve the
public API; minor releases may contain breaking changes, each marked
**Breaking** with its migration. Record subsequent changes under
`Unreleased`, identifying the module.

All modules require Go 1.27.3 or later. Released modules use published servex and kitx
versions without replacements: the releases of 2026-10-27 require servex
v1.2.0, cache/redis, encoding/proto and tracing/otel v1.0.0 require servex
v1.0.0. See [RELEASING.md](RELEASING.md) for release gates.

## Unreleased

- Development tooling: expose only check, test, build, lint, vulncheck, fmt,
  fix and tidy; make check the default and keep validation details internal.
  Remove redundant all/help/fix-check/clean task wrappers.

- Development tooling: consolidate unit, race, coverage, tooling regression and
  service tests under `task test`; select variants with `RACE`, `COVER` and
  `INTEGRATION`. CI matrix validation runs with ordinary tests, and `check`
  reuses the race variant.

- Development tooling: replace Python integration validators and regression
  fixtures with Go standard-library tools; no Python runtime is required.

- Development tooling (all modules): replace Make entry points with Task v3.53.1 in local
  checks, CI and contribution/release instructions. Use `task check`, `task lint`
  and `task vulncheck`; check coverage, build tags and failure gates are preserved.

- cache/redis, encoding/proto and tracing/otel: document standalone application
  setup, dependency tidying and nested-module version selection. These modules
  build unchanged against servex v1.2.0 and keep their v1.0.0 releases;
  encoding/proto's README example uses the v1.2.0 `httpx.With*` options and
  `httpx.RouteFunc`.

## di/do/v1.0.0

Release date: 2026-10-28. Requires servex v1.3.0.

- Implement core di.Container with samber/do/v2, lazy singleton construction,
  exact interface bindings, cycle detection and preserved error identities.
- Connect dependency-ordered shutdown and readiness to servex app.Hook and
  health.Checker. Supply transfers ownership even for unused resources.
- Include centralized do/Fx assembly examples; per-package module files are
  optional. servex remains the owner of server startup and shutdown.

## auth/oidc/v1.0.0

Release date: 2026-10-28. No servex dependency is required.

- Add OIDC discovery/JWKS and ID Token verification with issuer, audience and
  expiry validation for application authentication callbacks. This does not
  substitute for arbitrary OAuth access-token validation or introspection.

## ratelimit/redis/v1.0.0

Release date: 2026-10-28. Requires servex v1.3.0.

- Add shared Redis token buckets using server time, atomic quota checks,
  context cancellation, retry hints and expiring idle keys.
- Validate multi-replica quota behavior with a real disposable Redis fixture.

## middleware/v1.3.0

Release date: 2026-10-28. Requires servex v1.3.0.

- Add structural ContextLimiter and ratelimit.NewContext with backend errors,
  retry hints and explicit fail-open configuration. Preserve the local Limiter
  API; callers choose fail-open deliberately.

## transport/grpcx/v1.2.0

Release date: 2026-10-28. Requires servex v1.3.0.

- Add explicit server/client stream middleware chains with whole-stream
  observation, message counts, cancellation and managed client shutdown.
- Unary chains stay independent. Streams and messages are never replayed
  automatically; configure WithStreamMiddleware/WithClientStreamMiddleware.

## transport/grpcx/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Carry an explicit `errorx` retry decision across gRPC in the reserved
  `servex.retryable` ErrorInfo metadata key (`RetryableMetadataKey`), sent only
  when it differs from the code default; clients restore it and remove the key
  from `Metadata`. Statuses without the key keep code-derived retryability.
- Mark errors decoded by the client remote (`errorx.WithRemote`), including the
  statuses gRPC reports for connection failures; `Canceled` and
  `DeadlineExceeded` caused by the caller's own context stay local. The server
  encodes `PropagatedRetryable`, so a remote error returned unchanged or
  wrapped reaches the next caller as not retryable. **Breaking:** a service
  forwarding a downstream failure no longer invites its callers to retry; set
  `errorx.From(err).WithRetryable(true)` to restore that.
- Add the `WithName` server option (default `"grpc"`). **Breaking:** the server
  options `Address`, `Middleware`, `GRPCOptions` and `Health` are renamed
  `WithAddress`, `WithMiddleware`, `WithGRPCOptions` and `WithHealth`.
- Track connections of removed nodes with `transport.WithNodeObserver`, so idle
  connections close as soon as discovery reports the change.

## middleware/v1.2.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- tracing: follow the OpenTelemetry span status conventions. Server, internal
  and typed Command/Query spans call `RecordError` only for server faults
  (Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss);
  client-class codes keep the `servex.code` attribute without an error status.
  Client spans still record every failure. Add `IsServerFault`.
- metrics passes the call context to `Add` and `Observe`; retry documents that
  remote errors are retried only by the hop that received them. Examples use
  `endpoint.Typed` and `httpx.WithMiddleware`.

## database/mongodb/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Command metrics receive the context of the command span, or of the finished
  event when no started event was seen.

## metrics/prometheus/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- **Breaking:** `Counter.Add`, `Gauge.Set`, `Gauge.Add` and `Histogram.Observe`
  take a `context.Context` first, as the servex v1.2.0 contract requires; the
  context is not read.

## metrics/otel/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- **Breaking:** `Counter.Add`, `Gauge.Set`, `Gauge.Add` and `Histogram.Observe`
  take a `context.Context` first. It is passed to the OpenTelemetry
  instruments, so the SDK records exemplars carrying the trace and span IDs of
  a sampled span in the context.

## broker/kafka/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Implement `broker.ReadySubscriber`: group subscriptions are ready after their
  first group assignment (possibly empty), broadcast subscriptions once the
  cluster answered a ping. **Breaking:** `broker.NewConsumer` hooks block
  `Start` until ready and fail `Start` on setup errors such as an unreachable
  cluster; group consumers are named `consumer:<topic>:<group>`.

## broker/nats/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Implement `broker.ReadySubscriber`: ready once the consumer exists and its
  pull subscription is active. **Breaking:** `broker.NewConsumer` hooks block
  `Start` until ready and fail `Start` on setup errors such as a topic without
  a JetStream stream; group consumers are named `consumer:<topic>:<group>`.

## broker/rabbitmq/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Implement `broker.ReadySubscriber`: ready once the queue is declared and
  bound and the consumer accepted. **Breaking:** `broker.NewConsumer` hooks
  block `Start` until ready and fail `Start` on setup errors such as a queue
  declared with other arguments; group consumers are named
  `consumer:<topic>:<group>`.

## jobqueue/asynq/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Implement `jobqueue.ReadyRunner`: ready once Redis answered and the asynq
  server started, so a `jobqueue.NewWorker` hook fails `Start` when Redis is
  unreachable. Documentation and tests use `jobqueue.WithDelay`,
  `WithMaxRetry` and `WithUnique`.

## config/consul/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Examples and tests pass sources to `config.New` as a slice and show `Watch`
  options; watcher examples handle Ctrl+C.

## config/etcd/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Examples and tests pass sources to `config.New` as a slice and show `Watch`
  options; watcher examples handle Ctrl+C.

## config/nacos/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Examples and tests pass sources to `config.New` as a slice and show `Watch`
  options; watcher examples handle Ctrl+C.

## encoding/yaml/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Examples and tests pass sources to `config.New` as a slice; the example ships
  its configuration file.

## discovery/consul/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- A positive `Instance.Weight` becomes the passing weight of the service
  (warning weight 1); watchers report the passing weight, which Consul sets to
  1 by default. **Breaking:** watched instances carry `Weight` 1 instead of 0
  for services registered without a weight, and a weight change is reported
  as an update.

## discovery/etcd/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- `Instance.Weight` is stored in the `weight` JSON field and reported to
  watchers; values without the field read as weight 0.

## discovery/nacos/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- `Instance.Weight` becomes the Nacos weight (1 when not positive) and watchers
  report the Nacos weight of the first endpoint, rounded to an integer of at
  least 1. **Breaking:** watched instances carry weight 1 instead of 0 by
  default, and weights changed in the Nacos console reach weighted selectors.

## discovery/kubernetes/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- Document that EndpointSlices carry no weight (`Weight` 0); tests use the
  `httpx.With*` server options.

## selector/v1.1.0

Release date: 2026-10-27.

Requires servex v1.2.0.

- `p2c` prunes the state of removed nodes on the next `Select` after the node
  set shrank, also without new nodes. Both selectors honor the `Node.Weight`
  that servex v1.2.0 copies from discovery.

## middleware/v1.1.0

Release date: 2026-10-25.

- Add typed Command/Query logging, metrics and internal tracing using CQRS
  assembly metadata, independent of transport metadata. Existing endpoint APIs
  remain compatible.
- Exclude payloads, results, error text, error reasons and panic values from
  typed signals. Preserve results, error identity, cancellation and panic
  propagation; end internal spans once.
- Cover operation metadata, parent contexts, signal redaction and concurrent
  calls with regression tests and an executable command/query example.
- Require published servex v1.1.0, kitx v1.0.0 and Go 1.27.3 or later,
  without local replacements or development module proxies.

## broker/kafka/v1.0.0

Release date: 2026-10-23.

- Add Kafka publishing and subscribing with consumer groups, partition-based
  broadcast, handler retries and optional dead-letter topics through franz-go.

## broker/nats/v1.0.0

Release date: 2026-10-23.

- Add NATS JetStream publishing and subscribing using application-managed
  streams, with acknowledgment and redelivery handling.

## broker/rabbitmq/v1.0.0

Release date: 2026-10-23.

- Add AMQP 0-9-1 publishing and subscribing through amqp091-go, with
  publisher confirms and handler acknowledgment handling.

## cache/redis/v1.0.0

Release date: 2026-10-23.

- Add a Redis cache adapter over go-redis UniversalClient for standalone,
  sentinel and cluster deployments, usable with the servex cache Loader.

## config/consul/v1.0.0

Release date: 2026-10-23.

- Add Consul KV configuration loading and watching, supporting single-key
  documents and merged key prefixes.

## config/etcd/v1.0.0

Release date: 2026-10-23.

- Add etcd v3 configuration loading and watching, supporting single-key
  documents and merged key prefixes.

## config/nacos/v1.0.0

Release date: 2026-10-23.

- Add Nacos configuration loading and change notifications by data ID
  and group.

## database/mongodb/v1.0.0

Release date: 2026-10-23.

- Add MongoDB driver v2 command instrumentation, application lifecycle
  and health checks while retaining the native mongo.Client API.

## discovery/consul/v1.0.0

Release date: 2026-10-23.

- Add service registration, lookup and watching through the Consul agent
  API.

## discovery/etcd/v1.0.0

Release date: 2026-10-23.

- Add lease-backed etcd v3 service registration, lookup and watching.

## discovery/kubernetes/v1.0.0

Release date: 2026-10-23.

- Add service lookup and watching through Kubernetes EndpointSlices;
  registration remains managed by Kubernetes.

## discovery/nacos/v1.0.0

Release date: 2026-10-23.

- Add service registration, lookup and subscriptions through the Nacos
  naming service.

## encoding/proto/v1.0.0

Release date: 2026-10-23.

- Add a Protocol Buffers codec implementing the servex encoding contract
  for proto.Message values.

## encoding/yaml/v1.0.0

Release date: 2026-10-23.

- Add a YAML codec implementing the servex encoding contract, with
  string-keyed mappings for dynamically typed values.

## jobqueue/asynq/v1.0.0

Release date: 2026-10-23.

- Add Redis-backed task enqueueing and workers through asynq, adapting
  the servex jobqueue contracts.

## metrics/otel/v1.0.0

Release date: 2026-10-23.

- Add an OpenTelemetry metrics adapter for servex counters, gauges and
  histograms.

## metrics/prometheus/v1.0.0

Release date: 2026-10-23.

- Add Prometheus counters, gauges and histograms using a caller-supplied
  registerer.

## middleware/v1.0.0

Release date: 2026-10-23.

- Add protocol-independent middleware for endpoints, consumers and workers,
  with external capabilities supplied through functions and interfaces.
- Provide retries with capped exponential delays and equal jitter, using
  kitx backoff primitives without duration overflow.

## selector/v1.0.0

Release date: 2026-10-23.

- Add stateful node-selection strategies implementing the servex selector
  contract, using only servex, kitx and the standard library.

## tracing/otel/v1.0.0

Release date: 2026-10-23.

- Add OpenTelemetry tracer and context-propagation adapters for the servex
  tracing contracts.

## transport/grpcx/v1.0.0

Release date: 2026-10-23.

- Add gRPC server and client implementations of the servex transport
  contracts through gRPC-Go.
