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
