# servex-contrib

English | [简体中文](README.zh-CN.md)

Concrete implementations of the [servex](https://github.com/nostosx/servex)
contracts: backend adapters, codecs, selection algorithms, telemetry and
middleware. Core owns contracts and shared runtime mechanisms; implementations
live here even when they use only the standard library or kitx.

The 2026-10-29 coordinated release uses servex v1.4.0 for the extracted
configuration, logging and backend contracts. Published module versions below resolve
from the public Go proxy without source replacements. See the core
`BACKEND-MIGRATION.md` for changed import paths and constructors.

The 2026-10-28 releases are middleware v1.3.0, transport/grpcx v1.2.0 and
the initial v1.0.0 releases of auth/oidc, di/do and ratelimit/redis. These use
servex v1.3.0 where applicable; auth/oidc has no core dependency. Earlier modules
retain their independent versions and declared
core requirements. Those published releases resolve dependencies without sibling checkouts
or local replacements; see each version section in CHANGELOG.md.

Runnable applications composing core and contrib live in
[servex-examples](https://github.com/nostosx/servex-examples).

Since `middleware/v1.1.0` the middleware module provides typed CQRS logging,
metrics and tracing; see
[typed application middleware](middleware/README.md#typed-application-middleware).
Each module's CHANGELOG section lists its breaking changes and migration.

## Modules

Each directory below is its own Go module, split by dependency set so that
importing one implementation never pulls in the SDKs of another. Module paths
are `github.com/nostosx/servex-contrib/<directory>`. Production modules depend on public core contracts and explicit foundational
contrib implementations where needed. Keep dependencies directed and avoid
unrelated backend SDKs. Interoperability tests may compose implementations.

| Module | Version | Implements | Key dependency |
|---|---|---|---|
| [`cache/loader`](cache/loader) | v1.0.0 | Read-through cache loading with explicit codec, shared owner and write-back policy | standard library, core, kitx/singleflight |
| [`transport/client`](transport/client) | v1.0.0 | Shared discovery, selection and invocation engine for HTTP/gRPC adapters | standard library, core, kitx/backoff |
| [`transport/httpx`](transport/httpx) | v1.0.0 | HTTP transport server/client | net/http, JSON codec, selector |
| [`tracing/noop`](tracing/noop) | v1.0.0 | `tracing.Tracer` / `Propagator` no-op backend | standard library, core |
| [`metrics/noop`](metrics/noop) | v1.0.0 | `metrics.Meter` no-op backend | standard library, core |
| [`jobqueue/memory`](jobqueue/memory) | v1.0.0 | `jobqueue.Enqueuer` / `Runner` in memory | standard library, core, kitx |
| [`encoding/json`](encoding/json) | v1.0.0 | `encoding.Codec` JSON backend | encoding/json/v2 |
| [`discovery/memory`](discovery/memory) | v1.0.0 | `discovery.Registrar` / `Discoverer` in memory | standard library, core |
| [`database/sql`](database/sql) | v1.0.0 | database/sql tracing, metrics, lifecycle and readiness | standard library, core, no-op telemetry |
| [`cache/memory`](cache/memory) | v1.0.0 | `cache.Cache` in memory | kitx/ttlcache |
| [`broker/memory`](broker/memory) | v1.0.0 | `broker.Publisher` / `Subscriber` in memory | standard library, core, kitx |
| [`auth/jwt`](auth/jwt) | Unreleased | JWT signing and typed verification for application auth callbacks | `github.com/golang-jwt/jwt/v5` |
| [`auth/oauth2`](auth/oauth2) | v1.0.0 | PKCE code flow, bound one-time state, token refresh and client credentials | official `golang.org/x/oauth2` |
| [`auth/oauth2redis`](auth/oauth2redis) | Unreleased | Atomic shared OAuth2 state using the Session Redis pool | official `github.com/redis/go-redis/v9` |
| [`auth/login`](auth/login) | Unreleased | OAuth2/OIDC or password login, Session rotation and typed application principals | OAuth2, OIDC, Session and auth middleware |
| [`lifecycle`](lifecycle) | v1.0.0 | Explicit owned/borrowed resources and reverse-order assembly cleanup | standard library, core |
| [`auth/password`](auth/password) | v1.0.0 | Argon2id PHC password hashes, bounded verification and rehash policy | official `golang.org/x/crypto/argon2` |
| [`storage/s3`](storage/s3) | v1.0.0 | Bucket-scoped object CRUD, listing and presigned transfers | official AWS SDK for Go v2 |
| [`scheduler/cron`](scheduler/cron) | v1.0.0 | Context-aware in-process jobs, overlap suppression and application lifecycle | `github.com/robfig/cron/v3` |
| [`database/goose`](database/goose) | v1.0.0 | Explicit instance-scoped SQL migrations and PostgreSQL acceptance | `github.com/pressly/goose/v3` |
| [`coordination/etcd`](coordination/etcd) | v1.0.0 | Lease locks, election and fencing revisions | official etcd concurrency SDK |
| [`transport/cors`](transport/cors) | v1.0.0 | Explicit browser-origin policy and preflight handling | `github.com/rs/cors` |
| [`session`](session) | v1.0.0 | Server-side browser sessions, atomic token rotation and bounded memory store | standard library |
| [`session/redis`](session/redis) | Unreleased | Shared Session Store with atomic version checks, token rotation and TTL | official `github.com/redis/go-redis/v9` |
| [`database/gorm`](database/gorm) | v1.0.0 | Native GORM telemetry, lifecycle and readiness, with PostgreSQL acceptance | `gorm.io/gorm` |
| [`auth/oidc`](auth/oidc) | v1.0.0 | OIDC discovery, ID Token signature/JWKS verification for application auth callbacks | `github.com/coreos/go-oidc/v3` |
| [`broker/kafka`](broker/kafka) | v1.2.0 | `broker.Publisher` and `broker.Subscriber` on Apache Kafka | `github.com/twmb/franz-go` |
| [`broker/nats`](broker/nats) | v1.2.0 | `broker.Publisher` and `broker.Subscriber` on NATS JetStream | `github.com/nats-io/nats.go` |
| [`broker/rabbitmq`](broker/rabbitmq) | v1.2.1 | `broker.Publisher` and `broker.Subscriber` on RabbitMQ (AMQP 0-9-1) | `github.com/rabbitmq/amqp091-go` |
| [`cache/redis`](cache/redis) | v1.1.0 | `cache.Cache` on Redis (standalone, sentinel or cluster) | `github.com/redis/go-redis/v9` |
| [`config/consul`](config/consul) | v1.2.0 | `config.Source` and `config.Watchable` on the Consul KV store | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | v1.2.0 | `config.Source` and `config.Watchable` on etcd | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | v1.2.0 | `config.Source` and `config.Watchable` on the Nacos configuration service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`config/viper`](config/viper) | v1.0.1 | `config.Config`, file/env sources, snapshots and validation | `github.com/spf13/viper` |
| [`database/mongodb`](database/mongodb) | v1.2.0 | Tracing and metrics for the MongoDB driver, lifecycle Hook and health check | `go.mongodb.org/mongo-driver/v2` |
| [`di/do`](di/do) | v1.2.0 | `di.Container` on samber/do with lifecycle and readiness integration, plus centralized wiring examples | `github.com/samber/do/v2` |
| [`discovery/consul`](discovery/consul) | v1.2.0 | `discovery.Registrar` and `discovery.Discoverer` on the Consul agent | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | v1.2.0 | `discovery.Registrar` and `discovery.Discoverer` on etcd leases | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | v1.2.0 | `discovery.Discoverer` on Kubernetes EndpointSlices | `k8s.io/client-go` |
| [`discovery/nacos`](discovery/nacos) | v1.2.0 | `discovery.Registrar` and `discovery.Discoverer` on the Nacos naming service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/proto`](encoding/proto) | v1.1.0 | `encoding.Codec` named `proto` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | v1.2.0 | `encoding.Codec` named `yaml` | `go.yaml.in/yaml/v3` |
| [`jobqueue/asynq`](jobqueue/asynq) | v1.3.0 | `jobqueue.Enqueuer` and `jobqueue.Runner` on asynq (Redis) | `github.com/hibiken/asynq` |
| [`logger/slog`](logger/slog) | v1.0.0 | `logger.Logger` | `log/slog` |
| [`logger/zap`](logger/zap) | v1.0.0 | `logger.Logger` | `go.uber.org/zap` |
| [`metrics/otel`](metrics/otel) | v1.2.0 | `metrics.Meter` on an OpenTelemetry meter | `go.opentelemetry.io/otel/metric` |
| [`metrics/prometheus`](metrics/prometheus) | v1.2.0 | `metrics.Meter` on the Prometheus client | `github.com/prometheus/client_golang` |
| [`middleware`](middleware) | v1.5.0 | `endpoint.Middleware`: auth, circuitbreaker, logging, metrics, ratelimit, recovery, retry, timeout, tracing, validate | standard library, servex, kitx and no-op telemetry |
| [`ratelimit/rate`](ratelimit/rate) | v1.0.0 | Bounded keyed local token buckets for the generic rate-limit middleware | `golang.org/x/time/rate` |
| [`ratelimit/redis`](ratelimit/redis) | v1.0.0 | Atomic shared Redis token buckets with caller context, retry hints and backend errors | `github.com/redis/go-redis/v9` |
| [`selector`](selector) | v1.2.0 | `selector.Selector`: weighted random, roundrobin (smooth weighted), p2c (Power of Two Choices) | standard library, servex and kitx |
| [`tracing/otel`](tracing/otel) | v1.1.0 | `tracing.Tracer` and `tracing.Propagator` on OpenTelemetry | `go.opentelemetry.io/otel/trace` |
| [`transport/grpcx`](transport/grpcx) | v1.3.0 | `transport.Server` and a gRPC client with servex middleware, discovery and node selection | `google.golang.org/grpc` |
| [`transport/ssex`](transport/ssex) | v1.0.0 | SSE acceptor/sender and bounded streaming client | standard library |
| [`transport/websocketx`](transport/websocketx) | v1.0.0 | WebSocket handshakes, message I/O, Ping and close status | `github.com/coder/websocket` |
| [`transport/connectx`](transport/connectx) | v1.0.0 | Typed unary/streaming RPC over Connect, gRPC and gRPC-Web | `connectrpc.com/connect` |
| [`validation/playground`](validation/playground) | v1.0.0 | Struct-tag validation as `endpoint.Middleware` | `github.com/go-playground/validator/v10` |
| [`validation/protovalidate`](validation/protovalidate) | v1.0.0 | Protobuf schema/CEL validation as `endpoint.Middleware` | `buf.build/go/protovalidate` |

Validation SDKs and x/time/rate are owned only by these optional modules. Core
and generic middleware retain their existing dependency boundaries. Inject a
configured SDK validator into either validation adapter; pass the local rate
limiter to `middleware/ratelimit.New`. The SDK-free request-owned validation
and token-bucket implementations remain available in generic middleware.

Common conventions:

- Constructors take dependencies explicitly. Where an adapter accepts an SDK
  client you created, the caller retains ownership unless its API documents
  otherwise.
- Returned errors are classified with `errorx`: unreachable backends are
  retryable `Unavailable`, bad arguments `InvalidArgument`, and calls on a
  closed client, broker or queue a non-retryable `FailedPrecondition`.
- Every type that implements a servex contract has a compile-time assertion.

## Versioning

auth/oidc, di/do and ratelimit/redis are available at v1.0.0. Stream middleware
is available in transport/grpcx v1.2.0; ratelimit.NewContext is available in
middleware v1.3.0. Streams require explicit middleware selection and are never
replayed. OIDC verifies ID Tokens, not arbitrary OAuth access tokens.

Modules start at v1.0.0 and is versioned independently with
tags prefixed by its directory, for example `broker/kafka/v1.0.0` or
`middleware/v1.0.0`:

Run `go get` from your application's Go module. For a new application:

```sh
mkdir servex-demo
cd servex-demo
go mod init example.com/servex-demo
go get github.com/nostosx/servex-contrib/cache/redis@v1.1.0
```

Choose the module you need from the table, then save its complete Usage
example as `main.go`. Run `go mod tidy` and `go run .` in that application
directory after preparing the example's backend. Do not run these commands
from this repository root, which has no `go.mod`. For modules with multiple
complete programs, put each program in a separate directory.

Each module's release selects the servex version its CHANGELOG section names. Root tags such as
`v1.0.0` do not release these nested modules. The directory prefix belongs to
the Git tag; the version argument to `go get` remains `@v1.0.0`.
The CI workflow validates changes and does not publish releases.

## Running the examples

Usage examples use local development addresses. Start dedicated disposable
backends before running them; installing a Go module does not start its
service. The module READMEs describe required data, streams or cluster setup.
Programs that start a server or watch configuration keep running until you
press Ctrl+C. The middleware and selector examples demonstrate construction;
they do not start a server or make a request. The OpenTelemetry metrics
example records nothing until your application installs an SDK provider.

The examples omit production credentials, TLS and access policies. Configure
those on the SDK client you supply before using a remote backend. Where a
server listens on all interfaces, restrict its address or network access for
local testing. A discovery registration must advertise an address reachable
by its consumers; a loopback address is suitable only for clients on the
same host.

## Application composition

Assemble applications centrally in `app.go` with ordinary business constructors.
Use `auth/login` for browser authentication and `auth/oauth2redis` with the
same borrowed Redis client as `session/redis` and cache. Business handlers read
the application's principal through `middleware/auth.FromContext[P]`.
`database/gorm.OpenPostgres` exposes one `GORM()` / `SQL()` pool; pass `SQL()`
to Goose for explicit migration commands. `lifecycle.Scope` owns created SDK
resources and closes dependencies in reverse order, including construction or
startup failures. Borrowed clients remain owned by their caller.

`middleware/profile` shares logging, tracing and metrics across native HTTP,
unary RPCs, whole streams and workers. Add authentication and execution policies
explicitly in each scope. `di/do.ProvideConstructors` registers ordinary Go
constructors at assembly time. S3 listing returns store-relative keys, so each
listed key can be passed directly to Get/Delete. See the full application in
[servex-examples](https://github.com/nostosx/servex-examples).

These composition changes are unreleased. For their matching source checks use
`bash .github/scripts/check-source.sh check lint vulncheck MODULES='auth/login auth/oauth2redis'`.
The helper uses an external temporary modfile; canonical manifests contain no
source replacements or invented peer checksums. Public installation must be
verified after coordinated module publication.

## Development

See [performance measurements](PERFORMANCE.md) for HTTP/gRPC, codecs and common
middleware baselines, profiling findings, workload limits and reproduction commands.
The subsequent [optimization comparison](PERFORMANCE-OPTIMIZATION.md) records
behavior guards, allocation reductions and matched-binary timing results.

Use Go 1.27.3, Task v3.53.1 and Bash. Published releases resolve pinned module
versions without source replacements.

The repository root is not a
module. Run the common targets from this root; they apply to every module and
export `GOWORK=off`:

```sh
task check
task lint
task vulncheck
```

`check` rejects formatting, modernizer and tidy changes, vets and builds normal
and `integration` configurations, verifies the middleware, selector, cache/loader and transport/client dependency
boundaries, and runs shuffled race tests. `lint` and `vulncheck` also inspect
both build configurations. `task fmt`, `task fix` and `task tidy` explicitly
modify files. Use `MODULES` to select one or more modules:

```sh
MODULES='cache/redis' task check lint vulncheck
MODULES='middleware selector' task check
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for tool installation, architecture
boundaries and test requirements, [CHANGELOG.md](CHANGELOG.md) for unreleased
changes, [RELEASING.md](RELEASING.md) for module publication and
[SECURITY.md](SECURITY.md) for sensitive reports.

Applications can install the modules directly using `go get`; no local
replacement directives are needed. Each module README documents its public API.

### Tests

Unit tests need no external services: they use in-process fakes such as
`kfake`, an embedded NATS server, an embedded etcd, miniredis, the client-go
fake clientset, httptest servers and OpenTelemetry in-memory exporters.

Integration tests live in `*_integration_test.go` files behind the
`integration` build tag and read the server address from an environment
variable; a test whose variable is unset is skipped. The fixtures create and
remove their own topics, streams, keys or namespaces. A successful tagged
`go test` with missing variables proves only that the remaining tests passed;
it does not prove the service integrations ran.

| Variable | Modules | Example value |
|---|---|---|
| `SERVEX_IT_KAFKA_BROKERS` | `broker/kafka` | `127.0.0.1:9092` (comma separated) |
| `SERVEX_IT_NATS_URL` | `broker/nats` | `nats://127.0.0.1:4222` (JetStream enabled) |
| `SERVEX_IT_RABBITMQ_URL` | `broker/rabbitmq` | `amqp://guest:guest@127.0.0.1:5672/` |
| `SERVEX_IT_REDIS_ADDR` | `cache/redis`, `jobqueue/asynq`, `ratelimit/redis`, `session/redis`, `auth/oauth2redis` | `127.0.0.1:6379` |
| `SERVEX_IT_CONSUL_ADDR` | `config/consul`, `discovery/consul` | `127.0.0.1:8500` |
| `SERVEX_IT_ETCD_ENDPOINTS` | `config/etcd`, `discovery/etcd`, `coordination/etcd` | `127.0.0.1:2379` (comma separated) |
| `SERVEX_IT_NACOS_ADDR` | `config/nacos`, `discovery/nacos` | `127.0.0.1:8848` (HTTP API) |
| `SERVEX_IT_NACOS_GRPC_PORT` | `config/nacos`, `discovery/nacos` | optional, defaults to the HTTP port + 1000 |
| `SERVEX_IT_MONGODB_URI` | `database/mongodb` | `mongodb://127.0.0.1:27017` |
| `SERVEX_IT_POSTGRES_DSN` | `database/gorm`, `database/goose` | `host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable` |
| `SERVEX_IT_S3_ENDPOINT` | `storage/s3` | `http://127.0.0.1:9000` (owned MinIO fixture; see module README) |
| `SERVEX_IT_KUBECONFIG` | `discovery/kubernetes` | path to a kubeconfig file |

For example, after preparing a dedicated local Redis instance:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' task test INTEGRATION=true
```

Task does not provision services. The integration helper rejects missing
configuration, absent tests, skipped tests and failures. Modules without
service integration tests are omitted; the target fails if the selection
contains none. `task check` remains the separate unit and static check entry
point.

The [CI workflow](.github/workflows/ci.yml) defines an all-module check
matrix, independent lint and vulnerability jobs, nine disposable Docker
service jobs and a kind job for Kubernetes. A matrix check compares the matrices to the module
and integration-test directories. Each service job waits for readiness,
exports the required variable, runs only `TestIntegration*` tests and rejects
absent, skipped, failed or unfinished integration results. The service
container is removed even on failure; kind-action cleans up its cluster.
The jobs are independent and run on pull requests, pushes to `main` or manual
dispatch. Unreleased Session/login peer checks explicitly use temporary source
modfiles; released module checks resolve public dependencies. Refresh matching
dependency versions and checksums before release.
Passing unit checks does not substitute for service integration.

## License

Licensed under the [MIT License](LICENSE). Each module directory includes
a copy for its independently published archive.

The config/viper and logger/{slog,zap} modules implement the contracts in servex v1.4.0.

The 2026-10-29 extraction: nine backend modules are moved from core; weighted
random selection joins `selector/random` in the existing selector module.
The second phase moves cache/loader and transport/client into two additional
modules. App, health.Group, registration and thin Consumer/Worker lifecycle
adapters intentionally remain in core as the minimal framework runtime. See the core BACKEND-MIGRATION.md for API changes.
