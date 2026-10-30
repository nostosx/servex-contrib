# Contributing

Install [Task](https://taskfile.dev/docs/installation/) v3.53.1 outside the module,
matching CI, and put its binary on `PATH`:

```sh
go install github.com/go-task/task/v3/cmd/task@v3.53.1
```

Public tasks are `check`, `test`, `build`, `lint`, `vulncheck`, `fmt`, `fix`
and `tidy`. Bare `task` runs `check`. Validation helpers are internal; use
`task check` to run them. Run `task --list` to discover the public tasks. Tool overrides accept environment variables or
Task arguments, for example `task check GO="/path with spaces/go"`.
Checks force `GOWORK=off`; `check`, `lint` and `vulncheck` do not apply source fixes.

## Setup

Use Go 1.27.3, Task v3.53.1 and Bash. Race tests require a supported
platform and a working C toolchain. Clone this repository on its own.

The root is not a Go module. Each listed implementation has its own `go.mod`;
released versions resolve dependencies through the public Go module proxy.
The 2026-10-28 releases with a core dependency use servex v1.3.0; auth/oidc
needs no core. Other modules keep the requirements recorded in CHANGELOG.md.
Do not commit a `go.work` file or sibling replacements. Production cross-module
imports are allowed only for explicit shared capabilities such as JSON codecs,
selectors and no-op telemetry; keep the dependency graph directed.
Interoperability tests may depend on the owning config engine. Core must never
import contrib, and middleware may import only the two no-op telemetry backends
in addition to its standard-library/core/kitx dependencies.

Published module releases resolve their dependencies without sibling checkouts.
The 2026-10-29 extraction uses published servex v1.4.0 and independently
versioned contrib dependencies. See core's BACKEND-MIGRATION.md and validate
public versions without local replacements.

Install tools outside the modules with Go 1.27.3 and put them on `PATH`:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
```

Do not change global Go configuration to solve a checkout-specific problem.

## Checks

Run from the repository root:

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
MODULES='cache/redis' task check lint vulncheck
MODULES='middleware selector' task check
```

`task fmt`, `task fix` and `task tidy` explicitly modify files; review their
changes before rerunning checks. `GO`, `GOFMT`, `GOLANGCI_LINT` and `GOVULNCHECK`
can select installed executables.

`task test` includes module tests, integration-gate regression fixtures and
CI module/integration matrix validation without external services. Use
`RACE=true` for the race detector, `COVER=true` for race tests with per-module
coverage.out, and `INTEGRATION=true` for explicitly configured service tests.
`task check` reuses `task test RACE=true`.
The validators, CI matrix checks and tooling regressions use only the Go standard library.

## Protocol adapters

The ssex, websocketx and connectx v1.0.0 modules use published servex v1.4.0
protocol contracts. Run their normal standalone gates without source overrides:

```sh
task check lint vulncheck MODULES='transport/ssex transport/websocketx transport/connectx'
```

Loopback Go tests cover transport behavior. Real Chrome/WebKit acceptance lives
under tests/browser; keep its explicit local-source mode separate from public
module installation evidence. Browser streaming limitations and Origin/CORS
policy remain documented there.


## Service integrations

Prepare dedicated disposable services yourself; Task does not start them.
Export the matching `SERVEX_IT_*` variables from the
[README](README.md#tests), then select the modules to test:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' task test INTEGRATION=true
```

This target requires Bash and Go. It runs `TestIntegration*` tests
with the `integration` build tag and race detector, rejecting missing,
skipped, failed or unfinished tests. Compiling or scanning tagged code is
part of `task check`, but proves no service interaction. A direct tagged
`go test` can skip fixtures with missing variables and is not acceptance.
Do not test against shared or production services.

## Changes

Application composition belongs in the example application's `app.go`. Keep
ordinary business constructors independent of DI; `di/do.ProvideConstructors`
only resolves dependencies during assembly. `lifecycle.Scope` owns explicitly
created resources; borrowed clients have no implicit Close. Login composes
OAuth2/OIDC/Session/auth with a directed dependency graph, and oauth2redis borrows
the shared official Redis client. GORM PostgreSQL exposes one pool to GORM and
Goose; migrations remain explicit commands. Shared observation lives in
`middleware/profile` with protocol policies selected by the caller.

`auth/login` and `auth/oauth2redis` have unreleased peers. Run their checks with
`bash .github/scripts/check-source.sh check lint vulncheck MODULES='auth/login auth/oauth2redis'`.
Select modules explicitly. The helper creates and deletes a temporary external
modfile; do not commit local replacements, zero pseudo-versions or invented
public checksums. Source acceptance does not prove public installation.

The unreleased auth/oauth2, auth/password, storage/s3, database/goose,
scheduler/cron, coordination/etcd and transport/cors modules own their SDKs.
Do not import those SDKs into core or generic middleware. Constructors accept
explicit policy/clients and never select global engines. Database migrations
are explicitly invoked, never automatic per-replica startup writes. OAuth2 state
stores require atomic insert/consume; the built-in memory store is process-local.
Cron jobs require finite timeouts; SDK/client ownership remains with the caller.
Use standard-library CrossOriginProtection for CSRF composition. The native
asynq Scheduler adapter shares the existing client and does not elect a leader.
Run normal gates for each changed module, plus owned PostgreSQL/Redis/etcd/S3
integration fixtures where configured. See individual module READMEs for limits.

Production packages import public core contracts, their owning SDK and explicit
shared implementations when needed. Interoperability tests may compose implementations explicitly.
The core retains only its documented minimal framework runtime: App,
health.Group, registration and thin Consumer/Worker lifecycle adapters.
Shared target resolution, discovery-watch recovery and invocation orchestration
belong in transport/client; read-through, singleflight and write-back policies
belong in cache/loader. HTTP and gRPC depend on the same shared client engine.
Both new modules, including tests, may use only core, kitx and the standard library.
Optional SDK adapters live in separate modules: validation/playground owns
go-playground/validator, validation/protovalidate owns Buf Protovalidate and
ratelimit/rate owns golang.org/x/time/rate. They must not add SDK requirements
or imports to core or generic middleware. Rate composition tests import the
existing middleware contract from the adapter side. Callers configure validation
engines before concurrent use; an adapter never selects a global SDK instance.
auth/jwt owns golang-jwt/jwt/v5; its production code depends only on the SDK and
standard library. JWT composition tests import existing auth middleware from
the adapter side. Keep JWT SDK requirements and imports out of core and generic
middleware.
session implements browser sessions with only the standard library. Store writes
and rotations must be atomic and version-checked to prevent stale requests from
restoring revoked sessions. The bounded memory backend uses lazy expiry with no
cleanup worker. Run MODULES=session task check lint vulncheck.
session/redis owns the official go-redis client. Both Session modules are
unreleased; run `bash .github/scripts/check-session-redis.sh check lint vulncheck`
to resolve the matching source in a temporary external modfile. The canonical
session v1.0.0 requirement is the intended release, not public installation
evidence. The helper is also used by CI and the Redis integration gate. Remove
this temporary source path when the peer is published and verify public sums.
database/gorm owns the GORM plugin, SQL lifecycle hooks and PostgreSQL pool;
its native constructor uses official pgx and the GORM PostgreSQL driver.
Run the real backend gate with
SERVEX_IT_POSTGRES_DSN and MODULES=database/gorm against a disposable instance.
`selector` stays within standard library, servex and kitx. Middleware adds only
metrics/noop and tracing/noop to that set, including tests; keep all SDKs out. Keep third-party SDKs in the owning module.

Use clear Go names and English comments. Behavior fixes need focused
regressions with deterministic synchronization, explicit resource cleanup,
and coverage of cancellation, ownership and error identity where relevant.
Do not use arbitrary sleeps as the sole proof that concurrent work occurred.

Describe the problem, resulting behavior and checks in a pull request. Record
user-visible changes under the affected module in [CHANGELOG.md](CHANGELOG.md).
Sensitive reports follow [SECURITY.md](SECURITY.md). Module versions and
publication gates are documented in [RELEASING.md](RELEASING.md).

Verify middleware with GOWORK=off and the matching core and no-op telemetry
sources until the coordinated release. Published tags stay unchanged; release
changes under new module versions and repeat public-proxy consumer checks.

MongoDB 8 integration jobs use ubuntu-22.04 (kernel 6.8). The current newer
hosted kernels trigger MongoDB startup rejection before tests can run; see
[SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912). Keep the real
service readiness and no-skipped-integration gates enabled.
