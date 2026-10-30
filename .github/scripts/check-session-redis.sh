#!/usr/bin/env bash
# The new session peer is unreleased. Resolve this checkout only in a temporary
# modfile; canonical manifests keep their intended release dependency and no replace.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
cp "$root/session/redis/go.mod" "$temporary/session-redis.mod"
cp "$root/session/redis/go.sum" "$temporary/session-redis.sum"
cd "$root/session/redis"
export GOWORK=off
"${GO:-go}" mod edit -modfile="$temporary/session-redis.mod" \
  -replace="github.com/nostosx/servex-contrib/session=$root/session"
export GOFLAGS="${GOFLAGS:-} -modfile=$temporary/session-redis.mod"
export SERVEX_SESSION_SOURCE_MODFILE=1
# go/packages probes standard-library release tags with GO111MODULE=off. Go
# rejects -modfile in that probe; retain it for every actual module operation.
export SERVEX_SESSION_MODFILE="$temporary/session-redis.mod"
SERVEX_SESSION_GO="$(command -v "${GO:-go}")"
export SERVEX_SESSION_GO
mkdir "$temporary/bin"
cat > "$temporary/bin/go" <<'GO_WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
if [ "${GO111MODULE:-}" = off ]; then
  export GOFLAGS="${GOFLAGS//-modfile=$SERVEX_SESSION_MODFILE/}"
fi
exec "$SERVEX_SESSION_GO" "$@"
GO_WRAPPER
chmod +x "$temporary/bin/go"
export PATH="$temporary/bin:$PATH"
cd "$root"
task --silent "$@" MODULES=session/redis
