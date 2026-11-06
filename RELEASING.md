# Releasing servex-contrib

Each module is versioned independently, starting at v1.0.0. This includes
auth/oidc, di/do, di/fx and ratelimit/redis.
The CI workflow does not create tags or publish releases. Publication is a
separate maintainer decision.

## Dependency order

Core-dependent modules require published servex v1.7.0, including its HTTP
request decoder and native message/job handlers. Verify this core version from
the public proxy. Relocated directories define new contrib module paths, while
old tags retain their original snapshots. A local contrib-source pass is not
evidence that a selected module version can already be installed.

Capability modules generally keep their published paths: `auth/*`,
`session`, `session/redis`, `ratelimit/*` and `validation/*` continue their
existing version lines. Only directories renamed to match core package names
(see the CHANGELOG path table) start new module identities at v1.0.0. When a
renamed module is first published, retain old tags as immutable
historical snapshots. This batch does not publish old-path forwarding packages
or duplicate deprecation snapshots; the migration table names the new paths.

The OAuth2 consolidation is an explicit exception: clients move to the
`auth/oauth2/client` package and the former oauth2server module becomes
`auth/oauth2/server` in the parent OAuth2 module's v1.1.0 release. The Redis
adapter moves from oauth2redis to the nested `auth/oauth2/redis` module v1.0.0.
Publish the parent before Redis, then publish dependent auth/login. Retain all
old tags and include import migration instructions in the release notes.

Verify core and utility versions through the public Go proxy. Source validation
with temporary modfiles is useful during development; release acceptance must
resolve the actual selected module versions without overrides.
Contrib dependencies must form a directed graph. After the core, publish
foundational modules such as codecs, selectors, storage/cache/loader,
transport/client, database helpers and no-op telemetry before their dependent adapters. Refresh real dependency
versions and checksums, then verify each dependent module against the public
proxy without overrides. Backend composition tests may add module requirements;
include them when computing publication order.
The new di/fx module has only the published core and Fx dependencies; validate
its standalone installation independently. Default Fx documentation does not
make a new module path available before its own tag is published.
Update servex-examples only after the modules it imports are available.

## Module gates

For a coordinated extraction, validate and publish modules in dependency layers.
The manually dispatched `Release validation` workflow accepts a JSON `modules`
array and an `integrations` array of backend/modules objects. It runs the same
check, lint, vulnerability and real-service gates against the exact dispatched
revision using published dependencies only. Select every service backend used by
the batch, including `kind` for discovery/kubernetes. Require the complete batch
workflow to succeed before tagging its modules. After every layer is published,
the ordinary full-repository CI must also pass before downstream releases.
This avoids requiring unpublished downstream modules to validate an independent
foundational release; it never substitutes source replacements for public versions.

1. Choose the module version independently of the Go 1.27.3 toolchain.
   Review the selected module's notes and release date in
   [CHANGELOG.md](CHANGELOG.md). Move its entries from `Unreleased` into the
   matching dated batch's module/version table.
2. Run the module's checks with the published core dependency and
   `GOWORK=off`, without sibling replacement directives. From the repository
   root, for example:

   ```sh
   MODULES='storage/cache/redis' task check lint vulncheck
   ```

3. For modules with service-backed tests, provision dedicated disposable
   services, export the required addresses, and run the matching acceptance:

   ```sh
   SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='storage/cache/redis' task test INTEGRATION=true
   ```

   Require actual integration execution; skipped tests or compilation-only
   checks are insufficient. Record backend versions and platform coverage.
4. Require successful CI for the exact release revision. Check dependency and
   license changes, confirm the module directory includes its MIT `LICENSE`,
   and exclude credentials or local artifacts from release contents.

## Tags and published artifacts

Nested module tags include the complete directory prefix. For example,
`storage/cache/redis/v1.0.0` publishes
`github.com/nostosx/servex-contrib/storage/cache/redis@v1.0.0`;
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
