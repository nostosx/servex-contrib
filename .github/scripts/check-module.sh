#!/usr/bin/env bash
set -euo pipefail

# Compatibility entry point for callers already inside one module.
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
module="${PWD#"$root"/}"
if [ "$module" = "$PWD" ]; then
  echo 'Run check-module.sh from a module in this repository' >&2
  exit 1
fi
exec make -C "$root" MODULES="$module" fmt-check modernize-check tidy-check vet build deps-check
