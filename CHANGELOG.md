# Changelog

Modules are versioned independently with tags of the form `<module>/vX.Y.Z`.
All 21 modules are preparing their first release as v1.0.0; none has been
published yet. Published v1 releases preserve public API compatibility.
Record subsequent changes under `Unreleased`, identifying the module.

Every module requires Go 1.27.3 and resolves published kitx v1.0.0. The
servex core still uses a placeholder requirement and local replacement;
see [RELEASING.md](RELEASING.md) for dependency and publication gates.

## Unreleased

## broker/kafka/v1.0.0

Prepared first release; not yet published.

- Add Kafka publishing and subscribing with consumer groups, partition-based
  broadcast, handler retries and optional dead-letter topics through franz-go.

## broker/nats/v1.0.0

Prepared first release; not yet published.

- Add NATS JetStream publishing and subscribing using application-managed
  streams, with acknowledgment and redelivery handling.

## broker/rabbitmq/v1.0.0

Prepared first release; not yet published.

- Add AMQP 0-9-1 publishing and subscribing through amqp091-go, with
  publisher confirms and handler acknowledgment handling.

## cache/redis/v1.0.0

Prepared first release; not yet published.

- Add a Redis cache adapter over go-redis UniversalClient for standalone,
  sentinel and cluster deployments, usable with the servex cache Loader.

## config/consul/v1.0.0

Prepared first release; not yet published.

- Add Consul KV configuration loading and watching, supporting single-key
  documents and merged key prefixes.

## config/etcd/v1.0.0

Prepared first release; not yet published.

- Add etcd v3 configuration loading and watching, supporting single-key
  documents and merged key prefixes.

## config/nacos/v1.0.0

Prepared first release; not yet published.

- Add Nacos configuration loading and change notifications by data ID
  and group.

## database/mongodb/v1.0.0

Prepared first release; not yet published.

- Add MongoDB driver v2 command instrumentation, application lifecycle
  and health checks while retaining the native mongo.Client API.

## discovery/consul/v1.0.0

Prepared first release; not yet published.

- Add service registration, lookup and watching through the Consul agent
  API.

## discovery/etcd/v1.0.0

Prepared first release; not yet published.

- Add lease-backed etcd v3 service registration, lookup and watching.

## discovery/kubernetes/v1.0.0

Prepared first release; not yet published.

- Add service lookup and watching through Kubernetes EndpointSlices;
  registration remains managed by Kubernetes.

## discovery/nacos/v1.0.0

Prepared first release; not yet published.

- Add service registration, lookup and subscriptions through the Nacos
  naming service.

## encoding/proto/v1.0.0

Prepared first release; not yet published.

- Add a Protocol Buffers codec implementing the servex encoding contract
  for proto.Message values.

## encoding/yaml/v1.0.0

Prepared first release; not yet published.

- Add a YAML codec implementing the servex encoding contract, with
  string-keyed mappings for dynamically typed values.

## jobqueue/asynq/v1.0.0

Prepared first release; not yet published.

- Add Redis-backed task enqueueing and workers through asynq, adapting
  the servex jobqueue contracts.

## metrics/otel/v1.0.0

Prepared first release; not yet published.

- Add an OpenTelemetry metrics adapter for servex counters, gauges and
  histograms.

## metrics/prometheus/v1.0.0

Prepared first release; not yet published.

- Add Prometheus counters, gauges and histograms using a caller-supplied
  registerer.

## middleware/v1.0.0

Prepared first release; not yet published.

- Add protocol-independent middleware for endpoints, consumers and workers,
  with external capabilities supplied through functions and interfaces.
- Provide retries with capped exponential delays and equal jitter, using
  kitx backoff primitives without duration overflow.

## selector/v1.0.0

Prepared first release; not yet published.

- Add stateful node-selection strategies implementing the servex selector
  contract, using only servex, kitx and the standard library.

## tracing/otel/v1.0.0

Prepared first release; not yet published.

- Add OpenTelemetry tracer and context-propagation adapters for the servex
  tracing contracts.

## transport/grpcx/v1.0.0

Prepared first release; not yet published.

- Add gRPC server and client implementations of the servex transport
  contracts through gRPC-Go.
