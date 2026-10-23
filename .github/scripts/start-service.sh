#!/usr/bin/env bash
set -euo pipefail

backend="${1:?usage: start-service.sh backend}"
: "${GITHUB_ENV:?This CI helper exports addresses through GITHUB_ENV}"
container="servex-it-$backend"
# Each job owns one container; fixed ports are isolated by the hosted runner.
# Images are intentionally explicit rather than inheriting local shared infra.
case "$backend" in
  redis)
    docker run -d --name "$container" -p 127.0.0.1:6379:6379 redis:8
    setting='SERVEX_IT_REDIS_ADDR=127.0.0.1:6379'
    ;;
  mongodb)
    docker run -d --name "$container" -p 127.0.0.1:27017:27017 mongo:8
    setting='SERVEX_IT_MONGODB_URI=mongodb://127.0.0.1:27017'
    ;;
  rabbitmq)
    docker run -d --name "$container" -p 127.0.0.1:5672:5672 rabbitmq:4
    setting='SERVEX_IT_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/'
    ;;
  kafka)
    # No KAFKA_* overrides: the image's single-node KRaft default advertises
    # localhost:9092, matching the host port used by the Go tests.
    docker run -d --name "$container" -p 127.0.0.1:9092:9092 apache/kafka:4.3.1
    setting='SERVEX_IT_KAFKA_BROKERS=127.0.0.1:9092'
    ;;
  nats)
    docker run -d --name "$container" -p 127.0.0.1:4222:4222 -p 127.0.0.1:8222:8222 nats:2.14.2-alpine -js -m 8222
    setting='SERVEX_IT_NATS_URL=nats://127.0.0.1:4222'
    ;;
  consul)
    docker run -d --name "$container" -p 127.0.0.1:8500:8500 hashicorp/consul:1.20.1 agent -dev -client=0.0.0.0
    setting='SERVEX_IT_CONSUL_ADDR=127.0.0.1:8500'
    ;;
  etcd)
    docker run -d --name "$container" -p 127.0.0.1:2379:2379 quay.io/coreos/etcd:v3.7.2 \
      /usr/local/bin/etcd --name default --data-dir /tmp/servex-etcd \
      --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:2379
    setting='SERVEX_IT_ETCD_ENDPOINTS=127.0.0.1:2379'
    ;;
  nacos)
    # Nacos 2 SDKs need the HTTP and gRPC ports; auth is disabled only for
    # this disposable fixture, which is exposed to the runner's loopback.
    docker run -d --name "$container" -p 127.0.0.1:8848:8848 -p 127.0.0.1:9848:9848 \
      -e MODE=standalone -e NACOS_AUTH_ENABLE=false \
      -e JVM_XMS=256m -e JVM_XMX=512m -e JVM_XMN=128m nacos/nacos-server:v2.5.2
    setting='SERVEX_IT_NACOS_ADDR=127.0.0.1:8848'
    ;;
  *) printf 'Unknown backend: %s\n' "$backend" >&2; exit 1 ;;
esac

ready() {
  case "$backend" in
    redis) docker exec "$container" redis-cli ping | grep -qx PONG ;;
    mongodb) docker exec "$container" mongosh --quiet --eval 'if (!db.runCommand({ping: 1}).ok) { quit(1); }' ;;
    rabbitmq)
      docker logs "$container" 2>&1 | grep 'started TCP listener' >/dev/null
      ;;
    kafka) docker exec "$container" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 ;;
    nats) curl -fsS 'http://127.0.0.1:8222/healthz?js-enabled-only=true' ;;
    consul) curl -fsS http://127.0.0.1:8500/v1/status/leader | jq -e 'length > 0' ;;
    etcd) docker exec "$container" etcdctl --endpoints=http://127.0.0.1:2379 endpoint health ;;
    nacos) curl -fsS http://127.0.0.1:8848/nacos/v1/console/health/readiness ;;
  esac
}

for ((attempt = 1; attempt <= 90; attempt++)); do
  if ready >/dev/null 2>&1; then
    printf '%s\n' "$setting" >> "$GITHUB_ENV"
    printf '%s is ready\n' "$backend"
    exit 0
  fi
  running="$(docker inspect -f '{{.State.Running}}' "$container")"
  if [ "$running" != true ]; then break; fi
  sleep 2
done
docker logs "$container"
printf '::error::%s did not become ready\n' "$backend" >&2
exit 1
