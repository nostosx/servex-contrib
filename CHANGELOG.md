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

## config/viper/v1.0.1

Release date: 2026-10-29. Requires servex v1.4.0.

- Publish complete revisions atomically in the configuration file-watch test.
  In-place truncation can expose a valid empty configuration and falsely fail
  the reload assertion. Production configuration behavior and APIs are unchanged.

## discovery/consul/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## discovery/etcd/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## discovery/kubernetes/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## discovery/nacos/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## ratelimit/rate/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Add bounded keyed local token buckets using x/time/rate, reclaiming idle buckets without background goroutines.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/grpcx/v1.3.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Use the shared contrib client and assemble unary endpoint middleware once while preserving per-request metadata and status behavior.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/httpx/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move the HTTP backend from core; use the shared contrib client, bounded binding plans and fixed-path decoding optimizations.
- Resolve released dependencies from the public Go proxy without local replacements.

## cache/memory/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move the in-memory cache from core and compose it with the shared contrib Loader and explicit Codec.
- Resolve released dependencies from the public Go proxy without local replacements.

## cache/redis/v1.1.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## config/consul/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## config/etcd/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## config/nacos/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## database/mongodb/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## database/sql/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move native database/sql instrumentation, pool lifecycle and readiness from core with explicit telemetry backends.
- Resolve released dependencies from the public Go proxy without local replacements.

## discovery/memory/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move the in-memory registry from core and verify discovery with the shared contrib client.
- Resolve released dependencies from the public Go proxy without local replacements.

## encoding/yaml/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## metrics/otel/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## metrics/prometheus/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## middleware/v1.4.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Use explicit no-op telemetry backends and core Logger; reduce attribute allocation and skip timing with no logger.
- Resolve released dependencies from the public Go proxy without local replacements.

## broker/kafka/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## broker/memory/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move the in-memory broker implementation and readiness regressions from core.
- Resolve released dependencies from the public Go proxy without local replacements.

## broker/nats/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## broker/rabbitmq/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## cache/loader/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move typed read-through loading, owner cancellation, singleflight and explicit codec wiring from core into this module.
- Resolve released dependencies from the public Go proxy without local replacements.

## config/viper/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement core configuration contracts with Viper loading, decoding, validation, reload and immutable snapshots.
- Resolve released dependencies from the public Go proxy without local replacements.

## di/do/v1.1.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adapt centralized do assembly to core Logger and add startup/request-path performance benchmarks.
- Resolve released dependencies from the public Go proxy without local replacements.

## encoding/json/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Provide the encoding/json/v2 Codec formerly constructed in core.
- Resolve released dependencies from the public Go proxy without local replacements.

## encoding/proto/v1.1.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## jobqueue/asynq/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## jobqueue/memory/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move the in-memory job queue and readiness regressions from core.
- Resolve released dependencies from the public Go proxy without local replacements.

## logger/slog/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement the core Logger contract with an explicitly supplied slog backend.
- Resolve released dependencies from the public Go proxy without local replacements.

## logger/zap/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement the core Logger contract with an explicitly supplied Zap backend.
- Resolve released dependencies from the public Go proxy without local replacements.

## metrics/noop/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Provide an explicit no-op meter outside core.
- Resolve released dependencies from the public Go proxy without local replacements.

## selector/v1.2.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Add weighted random selection and keep algorithms outside core.
- Resolve released dependencies from the public Go proxy without local replacements.

## tracing/noop/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Provide explicit no-op tracing and propagation outside core; handle absent spans as nil.
- Resolve released dependencies from the public Go proxy without local replacements.

## tracing/otel/v1.1.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Adopt servex v1.4.0 contracts and matching explicit contrib dependencies; preserve backend behavior and refresh regression coverage.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/client/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Move discovery, selection, retries and protocol-independent invocation from core; require an explicit selector.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/connectx/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement typed Connect, gRPC and gRPC-Web unary/streaming RPC with metadata and structured error mapping.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/ssex/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement flushed SSE events/comments and bounded streaming parsing with resume cursors.
- Resolve released dependencies from the public Go proxy without local replacements.

## transport/websocketx/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Implement WebSocket handshakes, context-aware messages, Origin policy, Ping and close statuses.
- Resolve released dependencies from the public Go proxy without local replacements.

## validation/playground/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Add explicitly configured go-playground/validator endpoint validation without bringing the SDK into core or generic middleware.
- Resolve released dependencies from the public Go proxy without local replacements.

## validation/protovalidate/v1.0.0

Release date: 2026-10-29. Requires servex v1.4.0.

- Add explicitly configured Buf Protovalidate endpoint validation preserving schema/evaluation errors and violation causes.
- Resolve released dependencies from the public Go proxy without local replacements.

## Coordinated migration - 2026-10-29

- Add repeatable real-browser acceptance for SSE, WebSocket, Connect and
  gRPC-Web. Native EventSource/WebSocket and official Connect-Web clients run
  48 checks per Chrome/WebKit engine against race-enabled disposable HTTP
  fixtures. Cover same/allowed/denied origins, incremental delivery, SSE resume
  cursors, text/binary messages, metadata, structured/partial-response errors
  and actual server cancellation. Document application-owned CORS/Origin policy
  and the browser transports' unsupported request-streaming boundary.

- Optimize HTTP fixed-path body requests by skipping unused JSON field-map
  decoding while preserving the initial JSON validation, custom codec calls,
  stateful marshaler order and encoding error precedence.
- Cache immutable HTTP value/file binding plans per route and request type,
  retaining at most 128 types per route and binding uncached types normally.
  Preserve field promotion, recursive embedding, tags, text decoding, error
  order and body/query/path precedence without invoking factories eagerly.
- Compose gRPC unary endpoint middleware once at server construction while
  keeping request-specific handlers/metadata, health bypass and status mapping.
- Size log attribute slices for the complete record and skip timing with a nil
  logger, preserving level callbacks, field order and handler-only duration.

- Add validation/playground using go-playground/validator and
  validation/protovalidate using Buf Protovalidate. Both return endpoint
  middleware, preserve validation causes, distinguish constraint violations
  from input/schema/evaluation errors and check caller cancellation.
  Validators are explicitly constructed and configured by the caller.
- Add ratelimit/rate using golang.org/x/time/rate. Keyed local limits compose
  with the existing Limiter middleware contract, cap retained keys and reclaim
  only idle, fully replenished buckets without background goroutines.
- Keep the three SDKs out of core and generic middleware, including their
  module requirements. SDK imports are restricted to their owning modules;
  CI covers all three additions. These modules are published in the version sections above.

- Add independent cache/loader and transport/client modules for the shared
  read-through loader and protocol-independent client engine. Preserve owner
  cancellation, write-back, discovery recovery, middleware order and shutdown
  semantics; move the existing regression suites and update composition tests.
- **Breaking**: core cache.NewLoader/LoaderOption move to loader.New/Option;
  core transport.NewClient/ClientOption move to client.New/Option. Their With*
  options and Loader error values move with the implementations. HTTP/gRPC
  use the same shared client module. Use the matching released peer versions listed above.
- App, health.Group and thin Consumer/Worker lifecycle adapters intentionally
  stay in core. The new loader/client modules remain limited to core, kitx
  and standard-library dependencies, enforced by Task and lint boundaries.

- Extract nine backend modules from core: transport/httpx, database/sql,
  cache/memory, discovery/memory, broker/memory, jobqueue/memory,
  encoding/json, metrics/noop and tracing/noop. Original regression suites and
  backend-dependent composition tests move with the implementations.
- Add selector/random.New in the existing selector module. Shared client
  construction now requires an explicitly supplied Selector.
- **Breaking**: replace encoding.JSON with encoding/json.Codec,
  metrics.Noop with metrics/noop.New, and tracing.Noop/NoopPropagator with
  tracing/noop.New/NewPropagator. Old core implementation import paths disappear.
- HTTP/gRPC and other adapters select their concrete defaults in contrib.
  Foundational contrib dependencies are explicit and directed; middleware only
  adds the two no-op telemetry modules to its SDK-free dependency boundary.
- Logging and tracing consumers handle an absent core Span explicitly.
  Publish matching core and backend sources before public consumer validation.


- Add independent `transport/ssex`, `transport/websocketx` and
  `transport/connectx` modules implementing the new core protocol contracts.
  SSE supports flushed events/comments and bounded parsing; WebSocket supports
  context-aware messages, Origin policy, Ping, size limits and close statuses;
  Connect supports unary and all three stream shapes over Connect/gRPC/gRPC-Web,
  typed core boundaries, metadata and errorx/ErrorInfo mapping.
  They require the published protocol contracts in servex v1.4.0.
- transport/httpx: add `WithProtocols` to configure copied net/http protocol
  settings. `connectx.Protocols()` enables HTTP/1, HTTP/2 and h2c on the existing
  lifecycle-managed HTTP listener. Upgraded WebSocket connections remain owned
  by the application and require explicit shutdown.

- Add `config/viper`: implements the core configuration contracts using
  private Viper instances, ordered Sources, accepted snapshots, validators,
  cancellation-aware watches and reload error callbacks. Its `file`
  package uses Viper formats (JSON/YAML/TOML/env) and polling.
  Existing etcd, Consul and Nacos Sources compose with this implementation.
- Add independent `logger/slog` and `logger/zap` modules implementing
  `logger.Logger`; callers retain backend configuration and resource ownership.
- **Breaking** (middleware, jobqueue/asynq, metrics/otel, metrics/prometheus):
  loggers now use `logger.Logger`; wrap a concrete backend explicitly.
  Nil disables logging. Logging severity callbacks use `logger.Level`.
- **Breaking** (configuration): engine construction, Env, file loading,
  validation and snapshots moved out of core. Use `config/viper.New`,
  `viper.Env` and `config/viper/file.New`; watch callbacks take
  `config.Config`, validators take `config.Reader`. Conversion follows
  Viper/cast/mapstructure, not the former custom reflection decoder.
  JSON tags remain supported; squashed embedded fields follow mapstructure,
  nil is unset, numeric weak conversions can truncate or overflow, and error
  text changes. Validate ranges and invariants before publishing a snapshot.
- These adapters require the matching unreleased core; their declarations
  are preparation for coordinated release, not a claim of proxy availability.


- Normalize native Windows glob path separators before stripping go.mod in Task
  module discovery, and include the available modules in selection diagnostics.

- Normalize CRLF output from Windows module discovery before validating Task
  selections; preserve rejection of empty, unknown and outside module paths.

- Correct per-module installation tags, declared core requirements and API
  availability in module READMEs. No additional library release is needed.

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
