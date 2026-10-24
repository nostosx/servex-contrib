# servex-contrib

English | [简体中文](README.zh-CN.md)

Implementations of the [servex](https://github.com/nostosx/servex) contracts
that depend on third-party SDKs, plus two collections without third-party SDKs
(`middleware` and `selector`). The servex core holds the contracts and one
standard-library reference implementation each; every further implementation
lives here.

All modules use the published servex and kitx v1.0.0 dependencies. A
standalone checkout needs no sibling source directories or local replacements.

Runnable applications composing core and contrib live in
[servex-examples](https://github.com/nostosx/servex-examples).

## Modules

Each directory below is its own Go module, split by dependency set so that
importing one implementation never pulls in the SDKs of another. Module paths
are `github.com/nostosx/servex-contrib/<directory>`. Modules import only the
public servex packages and never each other.

| Module | Implements | Key dependency |
|---|---|---|
| [`broker/kafka`](broker/kafka) | `broker.Publisher` and `broker.Subscriber` on Apache Kafka | `github.com/twmb/franz-go` |
| [`broker/nats`](broker/nats) | `broker.Publisher` and `broker.Subscriber` on NATS JetStream | `github.com/nats-io/nats.go` |
| [`broker/rabbitmq`](broker/rabbitmq) | `broker.Publisher` and `broker.Subscriber` on RabbitMQ (AMQP 0-9-1) | `github.com/rabbitmq/amqp091-go` |
| [`cache/redis`](cache/redis) | `cache.Cache` on Redis (standalone, sentinel or cluster) | `github.com/redis/go-redis/v9` |
| [`config/consul`](config/consul) | `config.Source` and `config.Watchable` on the Consul KV store | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | `config.Source` and `config.Watchable` on etcd | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | `config.Source` and `config.Watchable` on the Nacos configuration service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`database/mongodb`](database/mongodb) | Tracing and metrics for the MongoDB driver, lifecycle Hook and health check | `go.mongodb.org/mongo-driver/v2` |
| [`discovery/consul`](discovery/consul) | `discovery.Registrar` and `discovery.Discoverer` on the Consul agent | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | `discovery.Registrar` and `discovery.Discoverer` on etcd leases | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | `discovery.Discoverer` on Kubernetes EndpointSlices | `k8s.io/client-go` |
| [`discovery/nacos`](discovery/nacos) | `discovery.Registrar` and `discovery.Discoverer` on the Nacos naming service | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/proto`](encoding/proto) | `encoding.Codec` named `proto` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | `encoding.Codec` named `yaml` | `go.yaml.in/yaml/v3` |
| [`jobqueue/asynq`](jobqueue/asynq) | `jobqueue.Enqueuer` and `jobqueue.Runner` on asynq (Redis) | `github.com/hibiken/asynq` |
| [`metrics/otel`](metrics/otel) | `metrics.Meter` on an OpenTelemetry meter | `go.opentelemetry.io/otel/metric` |
| [`metrics/prometheus`](metrics/prometheus) | `metrics.Meter` on the Prometheus client | `github.com/prometheus/client_golang` |
| [`middleware`](middleware) | `endpoint.Middleware`: auth, circuitbreaker, logging, metrics, ratelimit, recovery, retry, timeout, tracing, validate | standard library, servex and kitx |
| [`selector`](selector) | `selector.Selector`: roundrobin (smooth weighted), p2c (Power of Two Choices) | standard library, servex and kitx |
| [`tracing/otel`](tracing/otel) | `tracing.Tracer` and `tracing.Propagator` on OpenTelemetry | `go.opentelemetry.io/otel/trace` |
| [`transport/grpcx`](transport/grpcx) | `transport.Server` and a gRPC client with servex middleware, discovery and node selection | `google.golang.org/grpc` |

Common conventions:

- Constructors take dependencies explicitly. Where an adapter accepts an SDK
  client you created, the caller retains ownership unless its API documents
  otherwise.
- Returned errors are classified with `errorx`: unreachable backends are
  retryable `Unavailable`, bad arguments `InvalidArgument`, and calls on a
  closed client, broker or queue a non-retryable `FailedPrecondition`.
- Every type that implements a servex contract has a compile-time assertion.

## Versioning

Each of the 21 modules starts at v1.0.0 and is versioned independently with
tags prefixed by its directory, for example `broker/kafka/v1.0.0` or
`middleware/v1.0.0`:

Run `go get` from your application's Go module. For a new application:

```sh
mkdir servex-demo
cd servex-demo
go mod init example.com/servex-demo
go get github.com/nostosx/servex-contrib/cache/redis@v1.0.0
```

Choose the module you need from the table, then save its complete Usage
example as `main.go`. Run `go mod tidy` and `go run .` in that application
directory after preparing the example's backend. Do not run these commands
from this repository root, which has no `go.mod`. For modules with multiple
complete programs, put each program in a separate directory.

The dependency graph selects servex and kitx v1.0.0. Root tags such as
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

## Development

Clone this repository on its own; Go downloads the published dependencies.

Requires Go 1.27.3, GNU Make, Bash and Python 3. The repository root is not a
module. Run the common targets from this root; they apply to every module and
export `GOWORK=off`:

```sh
make check
make lint
make vulncheck
```

`check` rejects formatting, modernizer and tidy changes, vets and builds normal
and `integration` configurations, verifies the middleware and selector dependency
boundaries, and runs shuffled race tests. `lint` and `vulncheck` also inspect
both build configurations. `make fmt`, `make fix` and `make tidy` explicitly
modify files. Use `MODULES` to select one or more modules:

```sh
MODULES='cache/redis' make check lint vulncheck
MODULES='middleware selector' make check
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
| `SERVEX_IT_REDIS_ADDR` | `cache/redis`, `jobqueue/asynq` | `127.0.0.1:6379` |
| `SERVEX_IT_CONSUL_ADDR` | `config/consul`, `discovery/consul` | `127.0.0.1:8500` |
| `SERVEX_IT_ETCD_ENDPOINTS` | `config/etcd`, `discovery/etcd` | `127.0.0.1:2379` (comma separated) |
| `SERVEX_IT_NACOS_ADDR` | `config/nacos`, `discovery/nacos` | `127.0.0.1:8848` (HTTP API) |
| `SERVEX_IT_NACOS_GRPC_PORT` | `config/nacos`, `discovery/nacos` | optional, defaults to the HTTP port + 1000 |
| `SERVEX_IT_MONGODB_URI` | `database/mongodb` | `mongodb://127.0.0.1:27017` |
| `SERVEX_IT_KUBECONFIG` | `discovery/kubernetes` | path to a kubeconfig file |

For example, after preparing a dedicated local Redis instance:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' make integration
```

Make does not provision services. The integration helper rejects missing
configuration, absent tests, skipped tests and failures. Modules without
service integration tests are omitted; the target fails if the selection
contains none. `make check` remains the separate unit and static check entry
point.

The [CI workflow](.github/workflows/ci.yml) defines a 21-module check
matrix, independent lint and vulnerability jobs, eight disposable Docker
service jobs and a kind job for Kubernetes. A matrix check compares the matrices to the module
and integration-test directories. Each service job waits for readiness,
exports the required variable, runs only `TestIntegration*` tests and rejects
absent, skipped, failed or unfinished integration results. The service
container is removed even on failure; kind-action cleans up its cluster.
The jobs are independent and run on pull requests, pushes to `main` or manual
dispatch. Each job resolves the pinned published dependencies from a standalone
checkout. Passing unit checks does not substitute for service integration.

## License

Licensed under the [MIT License](LICENSE). Each module directory includes
a copy for its independently published archive.
