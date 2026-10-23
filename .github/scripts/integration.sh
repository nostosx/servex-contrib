#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
module="${1:?usage: integration.sh module-directory}"
case "$module" in
  broker/kafka) required=SERVEX_IT_KAFKA_BROKERS ;;
  broker/nats) required=SERVEX_IT_NATS_URL ;;
  broker/rabbitmq) required=SERVEX_IT_RABBITMQ_URL ;;
  cache/redis|jobqueue/asynq) required=SERVEX_IT_REDIS_ADDR ;;
  database/mongodb) required=SERVEX_IT_MONGODB_URI ;;
  config/consul|discovery/consul) required=SERVEX_IT_CONSUL_ADDR ;;
  config/etcd|discovery/etcd) required=SERVEX_IT_ETCD_ENDPOINTS ;;
  config/nacos|discovery/nacos) required=SERVEX_IT_NACOS_ADDR ;;
  discovery/kubernetes) required=SERVEX_IT_KUBECONFIG ;;
  *) printf 'Unknown integration module: %s\n' "$module" >&2; exit 1 ;;
esac
if [ -z "${!required:-}" ]; then
  printf 'Required variable is empty: %s\n' "$required" >&2
  exit 1
fi
if [ "$required" = SERVEX_IT_KUBECONFIG ]; then test -s "$SERVEX_IT_KUBECONFIG"; fi

result="$(mktemp)"
trap 'rm -f "$result"' EXIT
cd "$root/$module"
# Unit tests have their own job. Run the real-service tests explicitly so
# absent/skipped integration tests cannot be confused with unit-test success.
status=0
GOWORK=off "${GO:-go}" test -race -shuffle=on -count=1 -timeout=15m -tags integration -run '^TestIntegration' -json ./... | tee "$result" || status=$?
"${PYTHON:-python3}" "$root/.github/scripts/check-integration.py" "$result"
exit "$status"
