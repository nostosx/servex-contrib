# Contributing

## Setup

Use Go 1.27.3, GNU Make, Bash and Python 3. Race tests require a supported
platform and a working C toolchain. Clone this repository on its own.

The root is not a Go module. Each listed implementation has its own `go.mod`;
servex and kitx v1.0.0 resolve through the Go module proxy without sibling
checkouts or local replacements.
Do not commit a `go.work` file or add cross-module contrib imports.

Install tools outside the modules with Go 1.27.3 and put them on `PATH`:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
```

Do not change global Go configuration to solve a checkout-specific problem.

## Checks

Run from the repository root:

```sh
make check
make lint
make vulncheck
```

The Makefile exports `GOWORK=off` and applies each target to the modules.
`check` rejects formatting, modernizer and tidy changes, vets and builds
normal and `integration` configurations, checks dependency boundaries where
required, and runs shuffled race tests without test-result caching. `lint`
and `vulncheck` also inspect both build configurations. Unit checks require
no external services.

Filter any module-oriented target with a space-separated `MODULES` value:

```sh
MODULES='cache/redis' make check lint vulncheck
MODULES='middleware selector' make check
```

`make fmt`, `make fix` and `make tidy` explicitly modify files; review their
changes before rerunning checks. `GO`, `GOFMT`, `GOLANGCI_LINT` and `GOVULNCHECK`
can select installed executables.

`make tooling-check` runs the integration-gate regression fixtures without
external services; it is also part of `make check`. Python 3 is required.

## Service integrations

Prepare dedicated disposable services yourself; Make does not start them.
Export the matching `SERVEX_IT_*` variables from the
[README](README.md#tests), then select the modules to test:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' make integration
```

This target requires Bash, Python 3 and Go. It runs `TestIntegration*` tests
with the `integration` build tag and race detector, rejecting missing,
skipped, failed or unfinished tests. Compiling or scanning tagged code is
part of `make check`, but proves no service interaction. A direct tagged
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
