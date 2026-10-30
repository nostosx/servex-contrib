#!/usr/bin/env bash
# Explicit acceptance for unpublished peers. Canonical manifests are untouched.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
cd "$root"
modules=""
arguments=()
for argument in "$@"; do
  case "$argument" in MODULES=*) modules="${argument#MODULES=}" ;; *) arguments+=("$argument") ;; esac
done
if [ -z "$modules" ]; then printf 'Explicit MODULES selection required\n' >&2; exit 1; fi
go_binary="$(command -v "${GO:-go}")"
export SERVEX_SOURCE_GO="$go_binary" GOWORK=off
mkdir "$temporary/bin"
cat > "$temporary/bin/go" <<'GO_WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
if [ "${GO111MODULE:-}" = off ]; then export GOFLAGS="${GOFLAGS//"$SERVEX_SOURCE_FLAG"/}"; fi
exec "$SERVEX_SOURCE_GO" "$@"
GO_WRAPPER
chmod +x "$temporary/bin/go"
export PATH="$temporary/bin:$PATH"
original_flags="${GOFLAGS:-}"
for module in $modules; do
  test -f "$module/go.mod" || { printf 'Unknown module: %s\n' "$module" >&2; exit 1; }
  export SERVEX_SOURCE_MODFILE="$temporary/source.mod"
  # GOFLAGS uses Go's quoted-field syntax, not shell backslash escaping.
  export SERVEX_SOURCE_FLAG="'-modfile=$SERVEX_SOURCE_MODFILE'"
  cp "$module/go.mod" "$SERVEX_SOURCE_MODFILE"
  rm -f "$temporary/source.sum"
  if [ -f "$module/go.sum" ]; then cp "$module/go.sum" "$temporary/source.sum"; fi
  replacements=()
  for manifest in */go.mod */*/go.mod; do
    if [ -f "$manifest" ]; then
      directory="${manifest%/go.mod}"
      replacements+=("-replace=github.com/nostosx/servex-contrib/$directory=$root/$directory")
    fi
  done
  (cd "$module"; "$go_binary" mod edit -modfile="$SERVEX_SOURCE_MODFILE" "${replacements[@]}")
  export GOFLAGS="$original_flags $SERVEX_SOURCE_FLAG"
  task --silent "${arguments[@]}" MODULES="$module"
done
