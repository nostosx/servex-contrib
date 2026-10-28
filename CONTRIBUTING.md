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
Do not commit a `go.work` file or add cross-module contrib imports.

All modules, including `di/do`, resolve released dependencies without sibling
checkouts or local replacements.

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

Import only public servex packages and never another contrib module.
`middleware` and `selector` must use only the standard library, servex and kitx,
including test dependencies. Keep third-party SDKs in the owning module.

Use clear Go names and English comments. Behavior fixes need focused
regressions with deterministic synchronization, explicit resource cleanup,
and coverage of cancellation, ownership and error identity where relevant.
Do not use arbitrary sleeps as the sole proof that concurrent work occurred.

Describe the problem, resulting behavior and checks in a pull request. Record
user-visible changes under the affected module in [CHANGELOG.md](CHANGELOG.md).
Sensitive reports follow [SECURITY.md](SECURITY.md). Module versions and
publication gates are documented in [RELEASING.md](RELEASING.md).

Verify middleware changes with its published dependencies and GOWORK=off.
No development proxy or core source override is required. Published tags stay
unchanged; release changes under a new module version.
