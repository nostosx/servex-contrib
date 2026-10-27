# Changelog

Modules are versioned independently with tags of the form `<module>/vX.Y.Z`.
The initial release of each module is v1.0.0. Published v1 releases preserve
public API compatibility. Record subsequent changes under `Unreleased`,
identifying the module.

Published v1.0.0 modules require Go 1.27.3 or later and use servex and kitx
v1.0.0 without replacements. Middleware v1.1.0 uses servex v1.1.0 and kitx
v1.0.0. See [RELEASING.md](RELEASING.md) for release gates.

## Unreleased

- All modules: document standalone application setup, dependency tidying and
  nested-module version selection; complete example prerequisites and expected
  behavior, including seed data, persistent resources and Kubernetes deployment.
- config/consul, config/etcd and config/nacos: handle Ctrl+C in watcher examples.
- encoding/yaml: provide the configuration file used by the example.
- middleware and selector: include kitx in dependency-check instructions.
- middleware/tracing: follow the OpenTelemetry span status conventions. Server,
  internal and typed Command/Query spans call `RecordError` only for server
  faults (Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable,
  DataLoss); client-class codes such as NotFound, InvalidArgument,
  PermissionDenied or Canceled keep the `servex.code` attribute without an
  error status. Client spans still record every failure. Add `IsServerFault`.
- transport/grpcx: carry an explicit `errorx` retryability decision, such as
  `WithRetryable(false)` on Unavailable, in the reserved `servex.retryable`
  ErrorInfo metadata key (`RetryableMetadataKey`). It is sent only when it
  differs from the code default, any application value under that key is
  dropped, and clients restore it and remove the key from `Metadata`. Statuses
  without the key keep code-derived retryability.

### servex v1.2.0 adoption

The modules below adopt the unreleased servex v1.2.0, which changes v1 APIs in
place. Their next versions require servex v1.2.0; the breaking changes for
contrib users are marked **Breaking**. cache/redis and tracing/otel build
unchanged against v1.2.0; encoding/proto only updates its example to the
`httpx.With*` options and `httpx.RouteFunc`.

- transport/grpcx: errors decoded by the client are marked remote
  (`errorx.WithRemote`), including the statuses gRPC reports for connection
  failures; `Canceled` and `DeadlineExceeded` caused by the caller's own
  context stay local. The server encodes `PropagatedRetryable` under
  `servex.retryable`, so a remote error returned unchanged or wrapped reaches
  the next caller as not retryable. **Breaking:** a service forwarding a
  downstream failure no longer invites its callers to retry; set
  `errorx.From(err).WithRetryable(true)` to restore that. Add the `WithName`
  server option (default `"grpc"`). **Breaking:** the server options
  `Address`, `Middleware`, `GRPCOptions` and `Health` are renamed
  `WithAddress`, `WithMiddleware`, `WithGRPCOptions` and `WithHealth`, matching
  servex httpx. Connections of removed nodes are now
  tracked with `transport.WithNodeObserver`, so idle connections close as soon
  as discovery reports the change instead of on the next call.
- middleware: `metrics` passes the call context to `Add` and `Observe`;
  `retry` documents that remote errors are retried only by the hop that
  received them. Examples use `endpoint.Typed` and `httpx.WithMiddleware`.
- database/mongodb: command metrics receive the context of the command span,
  or of the finished event when no started event was seen.
- metrics/prometheus: **Breaking:** `Counter.Add`, `Gauge.Set`, `Gauge.Add` and
  `Histogram.Observe` take a `context.Context` first, as the servex v1.2.0
  contract requires; the context is not read.
- metrics/otel: **Breaking:** the same context parameter. It is passed to the
  OpenTelemetry instruments, so the SDK records exemplars carrying the trace
  and span IDs of a sampled span in the context; a nil context records
  without one.
- broker/kafka, broker/nats, broker/rabbitmq: implement
  `broker.ReadySubscriber`. Kafka group subscriptions are ready after their
  first group assignment (possibly empty) and broadcast subscriptions once the
  cluster answered a ping; NATS once the consumer exists and its pull
  subscription is active; RabbitMQ once the queue is declared and bound and
  the consumer accepted. **Breaking:** with servex v1.2.0, `broker.NewConsumer`
  hooks over these brokers block `Start` until ready and fail `Start` on setup
  errors, such as an unreachable cluster, a topic without a JetStream stream
  or a queue declared with other arguments, instead of failing the running
  application; group consumers are named `consumer:<topic>:<group>`.
- jobqueue/asynq: implement `jobqueue.ReadyRunner`; `RunReady` is ready once
  Redis answered and the asynq server started, so a `jobqueue.NewWorker` hook
  fails `Start` when Redis is unreachable. Documentation and tests use
  `jobqueue.WithDelay`, `WithMaxRetry` and `WithUnique`.
- config/consul, config/etcd, config/nacos, encoding/yaml: examples and tests
  pass sources to `config.New` as a slice and show `Watch` options.
- discovery/consul: a positive `Instance.Weight` becomes the passing weight of
  the service (warning weight 1); watchers report the passing weight, which
  Consul sets to 1 by default. **Breaking:** watched instances now carry
  `Weight` 1 instead of 0 for services registered without a weight, and a
  weight change is reported as an update.
- discovery/etcd: `Instance.Weight` is stored in the `weight` JSON field and
  reported to watchers; values without the field read as weight 0.
- discovery/nacos: `Instance.Weight` becomes the Nacos weight (1 when not
  positive) and watchers report the Nacos weight of the first endpoint,
  rounded to an integer of at least 1. **Breaking:** watched instances carry
  weight 1 instead of 0 by default, and Nacos weights changed in its console
  now reach weighted selectors.
- discovery/kubernetes: document that EndpointSlices carry no weight
  (`Weight` 0). Tests of all four discovery modules use the `httpx.With*`
  server options.
- selector: `p2c` prunes the state of removed nodes on the next `Select` after
  the node set shrank, also without new nodes. Both selectors honor the
  `Node.Weight` that servex v1.2.0 copies from discovery.

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
