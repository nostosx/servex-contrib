# Contributing

## Setup

Use Go 1.27.3, Task v3.53.1 and Bash. Race tests require a supported platform
and a C toolchain. Install tools outside library modules:

```sh
go install github.com/go-task/task/v3/cmd/task@v3.53.1
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
```

The root is not a Go module. Implementations have independent, recursively
discovered modules. Optional SDK adapters remain capability modules such as
auth, validation and ratelimit. Core-dependent modules require published servex
v1.7.0. Relocated contrib module paths remain unreleased; source checks map only
those contrib peers and do not require a local core checkout.

An ignored local go.work may join contrib modules for IDE navigation. Create
it with `go work init`, then add modules with `go work use -r .`. Until relocated
peers are published, also map their required versions in the workspace:

```sh
go work edit -replace=github.com/nostosx/servex-contrib/observability/metrics/noop@v1.0.0=./observability/metrics/noop
```

Apply the same version-specific mapping to other unpublished peers reported by
`go list -m -json all`. Do not commit workspaces or local go.mod replacements,
or change global Go settings for one checkout. Task gates force GOWORK=off.
Public tasks are check, test, build, lint, vulncheck, fmt, fix and tidy;
bare task runs check. Use `task --list` to discover them.

## Checks

Run from the repository root. Select exact modules while contrib peers remain
unpublished:

```sh
bash .github/scripts/check-source.sh check lint vulncheck \
  MODULES='middleware transport/httpx messaging/memory job/memory'
```

For all modules, obtain the current recursive module list:

```sh
modules=$(GOWORK=off go run .github/scripts/matrices/main.go -modules)
bash .github/scripts/check-source.sh check lint vulncheck MODULES="$modules"
```

The helper resolves published servex v1.7.0 and replaces only contrib peers in
external temporary manifests. Checked-in manifests and global Go configuration
stay unchanged; no go.work is created. Push/PR CI uses the same mode. This proves
compatibility with the published core and matching contrib source, not public
installation of unreleased contrib modules.

For deliberate changes to core and contrib together, the optional
`bash .github/scripts/local-peers.sh /absolute/path/to/servex task ...`
helper also replaces core. Report that development mode separately.
Release validation requires published dependencies without overrides; see
[RELEASING.md](RELEASING.md).

After publishing dependency layers and updating requirements/checksums, run the
standalone release gates without source overrides:

```sh
task check
task lint
task vulncheck
```

`Taskfile.yml` exports `GOWORK=off` and applies each target to the modules.
`check` rejects formatting, modernizer and tidy changes, vets and builds
normal and `integration` configurations, checks dependency boundaries where
required, and runs shuffled race tests without test-result caching. `lint`
and `vulncheck` also inspect both build configurations. Unit checks require
no external services.

Filter any module-oriented target with a space-separated `MODULES` value:

```sh
MODULES='storage/cache/redis' task check lint vulncheck
MODULES='middleware selector' task check
```

Selections name exact modules: `MODULES=middleware` does not select the independent
auth, validation or ratelimit adapters. `MODULES=transport/httpx` does not select
auth/login, session or session/redis. Select those module paths explicitly when
changing them.

`task fmt`, `task fix` and `task tidy` explicitly modify files; review their
changes before rerunning checks. `GO`, `GOFMT`, `GOLANGCI_LINT` and `GOVULNCHECK`
can select installed executables.

`task test` includes module tests, integration-gate regression fixtures and
CI module/integration matrix validation without external services. Use
`RACE=true` for the race detector, `COVER=true` for race tests with per-module
coverage.out, and `INTEGRATION=true` for explicitly configured service tests.
Coverage instruments every package within the selected module (`-coverpkg=./...`),
including execution by that module's cross-package tests. It does not count
dependency-module execution from another module's test suite. Compare profiles
using statement-weighted totals, not averages of package percentages. Real-service
integration execution is separate from the default coverage run.
`task check` reuses `task test RACE=true`.
The validators, CI matrix checks and tooling regressions use only the Go standard library.

## Protocol adapters

Select transport/ssex, transport/websocketx and transport/connectx for Go
loopback checks. Real browser acceptance in [tests/browser](tests/browser) uses
Google Chrome, Playwright WebKit and official Connect-Web clients:

```sh
cd tests/browser
npm ci --ignore-scripts
npx playwright install webkit
npm test
```

Requires Go 1.27.3, Node.js/npm, protoc and installed Google Chrome. GO and PROTOC
select executables. The fixture uses the published core version required by
transport/httpx; SERVEX_CORE optionally selects a development checkout instead.
BROWSERS=chrome or BROWSERS=webkit selects one engine; default is both.
BROWSER_OUTPUT selects report output (default: ignored results/).

The runner uses external temporary Go source replacements and random loopback
ports, cleans its servers and writes generated JS under ignored .generated/.
It checks SSE delivery/reconnection/cancellation; WebSocket frames, subprotocols
and clean close; Connect/gRPC-Web unary/server streams, structured errors,
headers/trailers, cancellation and Origin/CORS enforcement. Connect unary trailer
headers must be exposed; WebSocket Origin checking is separate from CORS.
Browser client/bidirectional streaming is rejected, not claimed as supported.
The suite uses HTTP/1.1 and does not establish browser TLS, reverse-proxy or
native Safari acceptance. Go tests separately cover HTTP/2 and h2c.
Reports include versions, assertions, console output, screenshots and fixture
logs; unexpected page errors, failed checks or Go races fail the command.

## Service integrations

Prepare dedicated disposable services; Task does not provision them. Export the
matching variables and select the modules:

| Variable | Modules | Example value |
|---|---|---|
| `SERVEX_IT_KAFKA_BROKERS` | `messaging/kafka` | `127.0.0.1:9092` (comma separated) |
| `SERVEX_IT_NATS_URL` | `messaging/nats` | `nats://127.0.0.1:4222` (JetStream enabled) |
| `SERVEX_IT_RABBITMQ_URL` | `messaging/rabbitmq` | `amqp://guest:guest@127.0.0.1:5672/` |
| `SERVEX_IT_REDIS_ADDR` | `storage/cache/redis`, `job/asynq`, `ratelimit/redis`, `session/redis`, `auth/oauth2redis` | `127.0.0.1:6379` |
| `SERVEX_IT_CONSUL_ADDR` | `config/consul`, `discovery/consul` | `127.0.0.1:8500` |
| `SERVEX_IT_ETCD_ENDPOINTS` | `config/etcd`, `discovery/etcd`, `coordination/etcd` | `127.0.0.1:2379` (comma separated) |
| `SERVEX_IT_NACOS_ADDR` | `config/nacos`, `discovery/nacos` | `127.0.0.1:8848` (HTTP API) |
| `SERVEX_IT_NACOS_GRPC_PORT` | `config/nacos`, `discovery/nacos` | optional, defaults to the HTTP port + 1000 |
| `SERVEX_IT_MONGODB_URI` | `storage/database/mongodb` | `mongodb://127.0.0.1:27017` |
| `SERVEX_IT_POSTGRES_DSN` | `storage/database/gorm`, `storage/database/goose` | `host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable` |
| `SERVEX_IT_S3_ENDPOINT` | `storage/s3` | `http://127.0.0.1:9000` (owned MinIO fixture; see module README) |
| `SERVEX_IT_KUBECONFIG` | `discovery/kubernetes` | path to a kubeconfig file |

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 \
  bash .github/scripts/check-source.sh test INTEGRATION=true MODULES='storage/cache/redis'
```

The gate runs TestIntegration* with the integration tag and race detector,
rejecting missing configuration and absent/skipped/failed/unfinished tests.
Modules without service tests are omitted; a selection containing none fails.
Direct tagged go test can skip unset services and is not integration acceptance.
Use the [PostgreSQL fixtures](storage/database/postgres/README.md#integration-fixtures)
for extension profiles. Never use shared or production services.

CI covers every module and integration directory. Service jobs wait for readiness
and clean owned fixtures on failure. MongoDB 8 uses ubuntu-22.04 because newer
hosted kernels can reject startup; see
[SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912).

## Changes

Core owns contracts and its documented minimal runtime. Implementations stay in
contrib, including those using only the standard library. Cross-module reuse
must remain explicit and directed. HTTP/gRPC share transport/client; cache loading
belongs in storage/cache/loader. Both, and selector, use only core, kitx and the
standard library, including tests. Generic middleware adds only the no-op
metrics/tracing backends. Keep SDKs in independent adapter modules, including
nested adapters. JWT production code uses only its SDK and the standard library;
interoperability tests compose from the adapter side. Configure SDKs before
concurrent use, without choosing global engines.

Component-owned configuration holds file-decodable fields, defaults and validation;
the application aggregates it. Inject SDK clients, TLS, callbacks and telemetry
through constructor arguments/options. Preserve explicit zero/false semantics.
Validate before acquiring resources, omit credentials from errors and arrange
assembly/startup cleanup. Accepted snapshots do not rebuild live resources.
Constructors do not load files, choose global backends or apply migrations.

Keep business constructors independent of DI. Assemble in app.go with native Fx,
manual wiring or optional samber/do; no per-package module.go or request-time
container lookup is required. Give resources one lifecycle owner; borrowed
resources stay caller-owned. See [lifecycle](lifecycle) and [di/fx](di/fx).

Each [middleware boundary](middleware) is one ordinary package with explicit
factories and native contracts. Execution wrappers live directly in their owning
boundary package. Share recording and option resolution through middleware/observe;
tokenauth and validation hold their named support capabilities; bulkhead owns
shared concurrency capacity.
Do not add internal directories or relocate complete middleware factories behind
forwarding packages. Keep tracing propagation within its boundary and ordering
in the application. Do not add preset builders,
implicit recovery/retries/transactions or cross-boundary factory forwarding.
Only transport/client.Policy owns outbound retries and call/attempt deadlines.
Message settlement and job completion stay with their backends.

Session writes/rotations and OAuth2 state consumption must remain atomic;
Session versions prevent stale writes from restoring revoked state. See owning
module READMEs for concurrency, ownership and security constraints.

Use clear Go names and English comments. Behavior fixes need focused regressions
with deterministic synchronization, cleanup and relevant cancellation/error-identity
coverage. Arbitrary sleeps alone do not prove concurrent work occurred.
PRs describe the problem, resulting behavior and checks. Record user-visible
changes in [CHANGELOG](CHANGELOG.md), follow [RELEASING](RELEASING.md) for publication,
and use [SECURITY](SECURITY.md) for private reports. Published tags stay unchanged.
