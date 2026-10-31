#!/usr/bin/env bash
# Explicit development mode: all overrides live in a disposable external directory.
set -euo pipefail
core="$(cd -- "${1:?usage: local-peers.sh /path/to/servex command [args...]}" && pwd -P)"
shift
test "$#" -gt 0
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
test -f "$core/go.mod"
real_go="$(command -v "${GO:-go}")"
real_lint="$(command -v "${GOLANGCI_LINT:-golangci-lint}" || true)"
real_vuln="$(command -v "${GOVULNCHECK:-govulncheck}" || true)"
temporary="$(mktemp -d)"
trap 'rm -rf -- "$temporary"' EXIT
declare -a manifests=()
for file in "$root"/*/go.mod "$root"/*/*/go.mod; do
  if [ -f "$file" ]; then manifests+=("$file"); fi
done
test "${#manifests[@]}" -gt 0
for file in "${manifests[@]}"; do
  module_dir="${file%/go.mod}"
  relative="${module_dir#"$root"/}"
  mkdir -p "$temporary/mods/$relative"
  cp "$file" "$temporary/mods/$relative/go.mod"
  if [ -f "$module_dir/go.sum" ]; then cp "$module_dir/go.sum" "$temporary/mods/$relative/go.sum"; fi
  replacements=("-replace=github.com/nostosx/servex=$core")
  for peer in "${manifests[@]}"; do
    peer_dir="${peer%/go.mod}"
    peer_name="${peer_dir#"$root"/}"
    replacements+=("-replace=github.com/nostosx/servex-contrib/$peer_name=$peer_dir")
  done
  (cd "$module_dir"; "$real_go" mod edit -modfile="$temporary/mods/$relative/go.mod" "${replacements[@]}")
done
consumer="${SERVEX_DEV_CONSUMER:-}"
if [ -n "$consumer" ]; then
  consumer="$(cd -- "$consumer" && pwd -P)"
  test -f "$consumer/go.mod"
  mkdir -p "$temporary/consumer"
  cp "$consumer/go.mod" "$temporary/consumer/go.mod"
  if [ -f "$consumer/go.sum" ]; then cp "$consumer/go.sum" "$temporary/consumer/go.sum"; fi
  (cd "$consumer"; "$real_go" mod edit -modfile="$temporary/consumer/go.mod" "${replacements[@]}")
fi
mkdir -p "$temporary/bin"
cat > "$temporary/bin/go" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
if [ "${GO111MODULE:-}" = off ]; then
  # go/packages probes the standard library in GOPATH mode; -modfile is invalid there.
  export GOFLAGS=''
elif [ -n "${SERVEX_DEV_CONSUMER:-}" ] && [ "$PWD" = "$SERVEX_DEV_CONSUMER" ]; then
  export GOFLAGS="${GOFLAGS:-} '-modfile=$SERVEX_DEV_CONSUMER_MODFILE'"
else
case "$PWD" in
  "$SERVEX_DEV_ROOT"/*)
    relative="${PWD#"$SERVEX_DEV_ROOT"/}"
    if [ -f "$SERVEX_DEV_MODFILES/$relative/go.mod" ]; then
      export GOFLAGS="${GOFLAGS:-} '-modfile=$SERVEX_DEV_MODFILES/$relative/go.mod'"
    fi
    ;;
esac
fi
case "${0##*/}" in
  go) binary="$SERVEX_DEV_GO_BIN" ;;
  golangci-lint) binary="$SERVEX_DEV_LINT_BIN" ;;
  govulncheck) binary="$SERVEX_DEV_VULN_BIN" ;;
  *) exit 1 ;;
esac
test -n "$binary" || { echo 'Requested check tool is not installed' >&2; exit 1; }
exec "$binary" "$@"
WRAPPER
chmod +x "$temporary/bin/go"
cp "$temporary/bin/go" "$temporary/bin/golangci-lint"
cp "$temporary/bin/go" "$temporary/bin/govulncheck"
export SERVEX_DEV_ROOT="$root" SERVEX_DEV_MODFILES="$temporary/mods" SERVEX_DEV_GO_BIN="$real_go"
export SERVEX_DEV_LINT_BIN="$real_lint" SERVEX_DEV_VULN_BIN="$real_vuln"
export SERVEX_DEV_CONSUMER="$consumer" SERVEX_DEV_CONSUMER_MODFILE="$temporary/consumer/go.mod"
export PATH="$temporary/bin:$PATH" GO="$temporary/bin/go" GOWORK=off
export GOLANGCI_LINT="$temporary/bin/golangci-lint" GOVULNCHECK="$temporary/bin/govulncheck"
cd "${consumer:-$root}"
"$@"
