# servex-contrib

English | [简体中文](README.zh-CN.md)

Concrete implementations of the [servex](https://github.com/nostosx/servex)
contracts: backend adapters, codecs, node selection, observability and middleware.
Core owns contracts and the minimal framework runtime; concrete implementations
live here even when they use only the standard library or kitx.

Use Go 1.27.3 and published **servex v1.7.0**. Each contrib module has an
independent version; the module/version and import migration tables are in
[CHANGELOG.md](CHANGELOG.md). Historical releases retain their original paths
and dependencies. A sibling servex checkout is not required.

## Layout

| Directory | Responsibility |
|---|---|
| `transport` | HTTP, gRPC, Connect, SSE, WebSocket, CORS and the shared client engine |
| `messaging` | Kafka, NATS, RabbitMQ and in-memory publish/subscribe backends |
| `job` | Asynq, PGMQ and in-memory job execution backends |
| `observability` | Independent logger, metrics and tracing implementations |
| `storage` | Cache, database, vector, text search and S3 object storage implementations |
| `config`, `discovery`, `selector` | Configuration sources, service discovery and node selection |
| `middleware` | Explicit policies for endpoint, application, client, messaging and job execution |
| `auth`, `session`, `ratelimit`, `validation` | Optional identity/login, browser session, quota and validation implementations |
| `scheduler`, `coordination` | In-process/database schedules, locks and election |
| `lifecycle`, `di/fx`, `di/do` | Resource ownership and native DI lifecycle bridges; Fx is the recommended application assembly |

## Modules

There are 65 independent Go modules. The repository root has no go.mod. Module
paths are `github.com/nostosx/servex-contrib/<directory>`; dependency sets stay
separate so selecting one backend does not import unrelated SDKs. Explicit shared
capabilities may be reused through a directed dependency graph.

Directories name capabilities, not the middleware boundary that consumes them.
Authentication SDKs live under `auth`, validation SDKs under `validation`,
rate-limit backends under `ratelimit` and browser sessions under `session`.
The generic middleware and HTTP modules do not import these optional SDKs;
select and assemble each required module explicitly.

| Module | Responsibility | Key dependency |
|---|---|---|
| [`config/consul`](config/consul) | `config.Source` and `config.Watchable` on the Consul KV store | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | `config.Source` and `config.Watchable` on etcd | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | `config.Source` and `config.Watchable` on the Nacos configuration service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`config/viper`](config/viper) | `config.Config`, file/env sources, snapshots and validation | `github.com/spf13/viper` |
| [`coordination/etcd`](coordination/etcd) | Lease locks, election and fencing revisions | official etcd concurrency SDK |
| [`di/do`](di/do) | Native samber/do lifecycle and readiness bridge; applications own assembly | github.com/samber/do/v2 |
| [`di/fx`](di/fx) | Recommended native Fx assembly and servex lifecycle integration | `go.uber.org/fx` |
| [`discovery/consul`](discovery/consul) | `discovery.Registrar` and `discovery.Discoverer` on the Consul agent | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | `discovery.Registrar` and `discovery.Discoverer` on etcd leases | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | `discovery.Discoverer` on Kubernetes EndpointSlices | `k8s.io/client-go` |
| [`discovery/memory`](discovery/memory) | `discovery.Registrar` / `Discoverer` in memory | standard library, core |
| [`discovery/nacos`](discovery/nacos) | `discovery.Registrar` and `discovery.Discoverer` on the Nacos naming service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/json`](encoding/json) | `encoding.Codec` JSON backend | encoding/json/v2 |
| [`encoding/proto`](encoding/proto) | `encoding.Codec` named `proto` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | `encoding.Codec` named `yaml` | `go.yaml.in/yaml/v3` |
| [`job/asynq`](job/asynq) | `job.Enqueuer` and `job.Runner` on asynq (Redis) | `github.com/hibiken/asynq` |
| [`job/memory`](job/memory) | `job.Enqueuer` / `Runner` in memory | standard library, core, kitx |
| [`job/pgmq`](job/pgmq) | Transactional enqueue, receipt fencing and durable workers | database/sql, PGMQ |
| [`lifecycle`](lifecycle) | Explicit owned/borrowed resources and reverse-order assembly cleanup | standard library, core |
| [`messaging/kafka`](messaging/kafka) | `messaging.Publisher` and `messaging.Subscriber` on Apache Kafka | `github.com/twmb/franz-go` |
| [`messaging/memory`](messaging/memory) | `messaging.Publisher` / `Subscriber` in memory | standard library, core, kitx |
| [`messaging/nats`](messaging/nats) | `messaging.Publisher` and `messaging.Subscriber` on NATS JetStream | `github.com/nats-io/nats.go` |
| [`messaging/rabbitmq`](messaging/rabbitmq) | `messaging.Publisher` and `messaging.Subscriber` on RabbitMQ (AMQP 0-9-1) | `github.com/rabbitmq/amqp091-go` |
| [`middleware`](middleware) | Boundary-specific logging, metrics, tracing and explicit authentication, validation, authorization, isolation and consistency policies | standard library, servex and no-op telemetry |
| [`auth/jwt`](auth/jwt) | JWT signing and typed verification for application auth callbacks | `github.com/golang-jwt/jwt/v5` |
| [`auth/oauth2`](auth/oauth2) | Client/server OAuth2, common login providers, OIDC, PKCE and PostgreSQL protocol storage | Go OAuth2 SDK / go-oidc / Ory Fosite |
| [`auth/oauth2/redis`](auth/oauth2/redis) | Atomic shared OAuth2 state using the Session Redis pool | official `github.com/redis/go-redis/v9` |
| [`auth/oidc`](auth/oidc) | OIDC discovery, ID Token signature/JWKS verification for application auth callbacks | `github.com/coreos/go-oidc/v3` |
| [`auth/password`](auth/password) | Argon2id PHC password hashes, bounded verification and rehash policy | official `golang.org/x/crypto/argon2` |
| [`auth/login`](auth/login) | OAuth2/OIDC or password login, Session rotation and typed application principals | OAuth2, OIDC, Session and auth middleware |
| [`session`](session) | Server-side browser sessions, atomic token rotation and bounded memory store | standard library |
| [`session/redis`](session/redis) | Shared Session Store with atomic version checks, token rotation and TTL | official `github.com/redis/go-redis/v9` |
| [`ratelimit/rate`](ratelimit/rate) | Bounded keyed local token buckets for the generic rate-limit middleware | `golang.org/x/time/rate` |
| [`ratelimit/redis`](ratelimit/redis) | Atomic shared Redis token buckets with caller context, retry hints and backend errors | `github.com/redis/go-redis/v9` |
| [`validation/playground`](validation/playground) | Struct-tag validation as `endpoint.Middleware` | `github.com/go-playground/validator/v10` |
| [`validation/protovalidate`](validation/protovalidate) | Protobuf schema/CEL validation as `endpoint.Middleware` | `buf.build/go/protovalidate` |
| [`observability/logger/slog`](observability/logger/slog) | `logger.Logger` | `log/slog` |
| [`observability/logger/zap`](observability/logger/zap) | `logger.Logger` | `go.uber.org/zap` |
| [`observability/metrics/noop`](observability/metrics/noop) | `metrics.Meter` no-op backend | standard library, core |
| [`observability/metrics/otel`](observability/metrics/otel) | `metrics.Meter` on an OpenTelemetry meter | `go.opentelemetry.io/otel/metric` |
| [`observability/metrics/prometheus`](observability/metrics/prometheus) | `metrics.Meter` on the Prometheus client | `github.com/prometheus/client_golang` |
| [`observability/tracing/noop`](observability/tracing/noop) | `tracing.Tracer` / `Propagator` no-op backend | standard library, core |
| [`observability/tracing/otel`](observability/tracing/otel) | `tracing.Tracer` and `tracing.Propagator` on OpenTelemetry | `go.opentelemetry.io/otel/trace` |
| [`scheduler/cron`](scheduler/cron) | Context-aware in-process jobs, overlap suppression and application lifecycle | `github.com/robfig/cron/v3` |
| [`scheduler/pgcron`](scheduler/pgcron) | Persistent named SQL schedules | database/sql, pg_cron |
| [`selector`](selector) | `selector.Selector`: weighted random, roundrobin (smooth weighted), p2c (Power of Two Choices) | standard library and servex |
| [`storage/cache/loader`](storage/cache/loader) | Read-through cache loading with explicit codec, shared owner and write-back policy | standard library, core, kitx/singleflight |
| [`storage/cache/memory`](storage/cache/memory) | `cache.Cache` in memory | kitx/ttlcache |
| [`storage/cache/redis`](storage/cache/redis) | `cache.Cache` on Redis (standalone, sentinel or cluster) | `github.com/redis/go-redis/v9` |
| [`storage/database/goose`](storage/database/goose) | Explicit instance-scoped SQL migrations and PostgreSQL acceptance | `github.com/pressly/goose/v3` |
| [`storage/database/gorm`](storage/database/gorm) | Native GORM telemetry, lifecycle and readiness, with PostgreSQL acceptance | `gorm.io/gorm` |
| [`storage/database/mongodb`](storage/database/mongodb) | Tracing and metrics for the MongoDB driver, lifecycle Hook and health check | `go.mongodb.org/mongo-driver/v2` |
| [`storage/database/postgres`](storage/database/postgres) | Driver-neutral SQL helpers and explicit extension administration | database/sql |
| [`storage/database/sql`](storage/database/sql) | database/sql tracing, metrics, lifecycle and readiness | standard library, core, no-op telemetry |
| [`storage/s3`](storage/s3) | Bucket-scoped object CRUD, listing and presigned transfers | official AWS SDK for Go v2 |
| [`storage/search/postgres`](storage/search/postgres) | Native, ParadeDB, PGroonga and trigram text search | database/sql |
| [`storage/vector/pgvector`](storage/vector/pgvector) | Vector records, exact/ANN retrieval and caller transactions | database/sql, pgvector |
| [`transport/client`](transport/client) | Shared discovery, selection and invocation engine for HTTP/gRPC adapters | standard library, core, kitx/backoff |
| [`transport/connectx`](transport/connectx) | Typed unary/streaming RPC over Connect, gRPC and gRPC-Web | `connectrpc.com/connect` |
| [`transport/cors`](transport/cors) | Explicit browser-origin policy and preflight handling | `github.com/rs/cors` |
| [`transport/grpcx`](transport/grpcx) | `transport.Server` and a gRPC client with servex middleware, discovery and node selection | `google.golang.org/grpc` |
| [`transport/httpx`](transport/httpx) | HTTP transport server/client | net/http, JSON codec, selector |
| [`transport/ssex`](transport/ssex) | SSE acceptor/sender and bounded streaming client | standard library |
| [`transport/websocketx`](transport/websocketx) | WebSocket handshakes, message I/O, Ping and close status | `github.com/coder/websocket` |

## Configuration

Component configuration belongs to its implementation module. Applications own
only the aggregate; core retains configuration contracts. The foundational APIs are:

| Component | File-decodable model | Construction |
|---|---|---|
| slog / Zap | `Config`, `DefaultConfig`, `Validate` | `New(config)`; `Wrap(native)` for an existing backend |
| GORM PostgreSQL | `DatabaseConfig`, `DefaultDatabaseConfig`, `Validate` | `OpenPostgres(name, config, tracer, meter, options...)` |
| database/sql | `DatabaseConfig`, `DefaultDatabaseConfig`, `Validate` | `OpenFromConfig(config, options...)` |
| HTTP / gRPC server | `ServerConfig`, `DefaultServerConfig`, `Validate` | `NewServerFromConfig(config, options...)` |
| Redis cache | `Config`, `DefaultConfig`, `Validate` | `Open(config, options...)`; `New(client)` borrows a pool |

Start with defaults, then decode the merged file/environment snapshot. Omitted
fields retain defaults; explicit zero/false values retain each component's
documented meaning. These fragments use the contrib slog, GORM and HTTP packages:

```go
type Config struct {
    Log      slog.Config         `json:"log"`
    Database gorm.DatabaseConfig `json:"database"`
    HTTP     httpx.ServerConfig  `json:"http"`
}

cfg := Config{
    Log: slog.DefaultConfig(),
    Database: gorm.DefaultDatabaseConfig(),
    HTTP: httpx.DefaultServerConfig(),
}
if err := settings.Snapshot().Unmarshal("", &cfg); err != nil { return err }
if err := errors.Join(cfg.Log.Validate(), cfg.Database.Validate(), cfg.HTTP.Validate()); err != nil {
    return err
}
```

`settings` comes from [config/viper](config/viper), with file.New("config.toml")
and cfgviper.Env("APP_"). `APP_DATABASE__DSN` overrides database.dsn. Duration
strings such as "30m" are decoded by Viper; JSON tags define the field names.
For reload, repeat default/decode/validate in WithValidator before publishing.
Changes to live pools, listeners and log outputs require explicit application
handling; accepting a snapshot does not recreate resources.

Runtime loggers, Writers, Tracers, Meters, TLS policies, native GORM configuration,
callbacks and middleware are constructor arguments/options. Provide selected
component configs to native Fx and construct resources in ordinary application
providers. Register cleanup immediately, including assembly/startup failure paths;
Close/Hook ownership is described in each component README.

## Application composition

Use native Fx in the application's `app.go`, with ordinary constructors and one
lifecycle owner per resource. [di/fx](di/fx) bridges Fx to servex startup/shutdown;
manual construction and [di/do](di/do) remain available.

Select and order policies explicitly. [middleware](middleware) provides one
ordinary package per boundary: endpoint, application, client, messaging and job.
Their contracts distinguish typed business handlers, whole outbound calls,
selected-node attempts and native message/job outcomes. SDK adapters remain
independent modules; no preset chain is installed.

See [servex-examples](https://github.com/nostosx/servex-examples) for runnable
applications and [storage/database/postgres](storage/database/postgres) for
PostgreSQL capability ownership and integration fixtures. Existing applications
need to migrate imports and dependencies to the current APIs.

## Development

Use Go 1.27.3, Task v3.53.1 and Bash. From the contrib root, select exact
modules:

```sh
task check lint vulncheck \
  MODULES='middleware transport/httpx'
```

Checks resolve the versions declared in each module without source overrides.
See [CONTRIBUTING](CONTRIBUTING.md) for all-module selection, tools, workspaces,
service integrations and browser acceptance; [RELEASING](RELEASING.md) for
publication; [SECURITY](SECURITY.md) for private vulnerability reports.

### Tests

Unit tests need no external services. Service tests use the integration tag and
explicit disposable fixtures; skipped tests are not integration acceptance.
Commands and environment variables are maintained in the
[contribution guide](CONTRIBUTING.md#service-integrations).

## License

Licensed under the [MIT License](LICENSE). Each module directory includes its
own copy for the independently published archive.
