# Releasing servex-contrib

Each module is versioned independently, starting at v1.0.0. This includes
auth/oidc, di/do and ratelimit/redis.
The CI workflow does not create tags or publish releases. Publication is a
separate maintainer decision.

## Dependency order

The 2026-10-28 modules with a core dependency use published servex v1.3.0;
auth/oidc has no core requirement. Other modules keep their recorded dependency
versions. Where required, kitx remains v1.0.0. Those released snapshots require
no local replacements. Current unreleased extractions require matching sources
through temporary modfiles until the coordinated publication.
Before adopting a newer core or utility release, verify that version through
the public Go proxy, update requirements, tidy and rerun the module gates.
Contrib dependencies must form a directed graph. After the core, publish
foundational modules such as cache/loader, transport/client, codecs, selectors
and no-op telemetry before their dependent adapters. Refresh real dependency
versions and checksums, then verify each dependent module against the public
proxy without overrides. Backend composition tests may add module requirements;
include them when computing publication order.
Update servex-examples only after the modules it imports are available.

## Module gates

1. Choose the module version independently of the Go 1.27.3 toolchain.
   Review the selected module's notes and release date in
   [CHANGELOG.md](CHANGELOG.md). Move its entries from `Unreleased` into the
   matching dated module/version section.
2. Run the module's checks with the published core dependency and
   `GOWORK=off`, without sibling replacement directives. From the repository
   root, for example:

   ```sh
   MODULES='cache/redis' task check lint vulncheck
   ```

3. For modules with service-backed tests, provision dedicated disposable
   services, export the required addresses, and run the matching acceptance:

   ```sh
   SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' task test INTEGRATION=true
   ```

   Require actual integration execution; skipped tests or compilation-only
   checks are insufficient. Record backend versions and platform coverage.
4. Require successful CI for the exact release revision. Check dependency and
   license changes, confirm the module directory includes its MIT `LICENSE`,
   and exclude credentials or local artifacts from release contents.

## Tags and published artifacts

Nested module tags include the complete directory prefix. For example,
`cache/redis/v1.0.0` publishes
`github.com/nostosx/servex-contrib/cache/redis@v1.0.0`;
`middleware/v1.0.0` publishes the middleware module. A root `v1.0.0` tag
does not publish the nested modules. Follow Go's major-version module-path
rules for v2 and later.

After the gates pass and publication is authorized, create the selected
module tag and publish notes for that module. Verify from a fresh external
consumer module that the version can be fetched, imported and built without
local replacements or sibling checkouts. Inspect the module archive for its
license and expected files. Local sibling-based CI does not establish that
a published dependency graph is installable.

Do not move a published tag to fix a release. Publish a corrected version,
document the impact and use a module retraction when appropriate.

For middleware v1.1.0, verify servex v1.1.0 through the public proxy before
publishing `middleware/v1.1.0`. Keep the existing middleware and core v1.0.0
tags unchanged. Verify the new middleware version from an independent consumer
after publication.
