#!/usr/bin/env bash
# Runs inside an owned, disposable postgres:17 container. No image build.
set -euo pipefail
profile="${1:?profile required}"
export DEBIAN_FRONTEND=noninteractive
printf '%s\n' 'c10585ac22c7f617a73d394eea29ba71d0356f350ae973b19043d1b017f80f54  /tmp/servex-ca.deb' | sha256sum -c -
dpkg-deb -x /tmp/servex-ca.deb /tmp/servex-ca
cat /tmp/servex-ca/usr/share/ca-certificates/mozilla/*.crt > /tmp/servex-ca.crt
printf '%s\n' 'Acquire::https::CaInfo "/tmp/servex-ca.crt";' > /etc/apt/apt.conf.d/servex-tls
for source_file in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
  if [ -f "$source_file" ]; then
    sed -i 's|http://deb.debian.org|https://deb.debian.org|g;s|http://apt.postgresql.org|https://apt.postgresql.org|g' "$source_file"
  fi
done
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl unzip
# shellcheck disable=SC1091
. /etc/os-release
if [ "$VERSION_CODENAME" != trixie ]; then
  echo 'This fixture requires Debian trixie; select a compatible PostgreSQL 17 image.' >&2
  exit 1
fi
curl --retry 3 -fsSL https://repo.pigsty.cc/key -o /usr/share/keyrings/pigsty.asc
printf '%s\n' 'deb [signed-by=/usr/share/keyrings/pigsty.asc] https://repo.pigsty.cc/apt/pgsql/trixie trixie main' > /etc/apt/sources.list.d/pigsty.list
apt-get update
case "$profile" in
  base)
    curl --retry 3 -fsSL "https://packages.groonga.org/debian/groonga-apt-source-latest-${VERSION_CODENAME}.deb" -o /tmp/groonga.deb
    apt-get install -y /tmp/groonga.deb
    apt-get update
    apt-get install -y --no-install-recommends postgresql-17-pgvector postgresql-17-cron \
      postgresql-17-postgis-3 postgresql-17-partman postgresql-17-pgaudit \
      postgresql-17-hypopg postgresql-17-pgroonga
    arch="$(dpkg --print-architecture)"
    curl --retry 3 -fsSL "https://github.com/paradedb/paradedb/releases/download/v0.26.4/postgresql-17-pg-search_0.26.4-1PARADEDB-${VERSION_CODENAME}_${arch}.deb" -o /tmp/pgsearch.deb
    apt-get install -y /tmp/pgsearch.deb
    curl --retry 3 -fsSL https://raw.githubusercontent.com/pgmq/pgmq/v1.13.1/pgmq-extension/pgmq.control -o /usr/share/postgresql/17/extension/pgmq.control
    curl --retry 3 -fsSL https://raw.githubusercontent.com/pgmq/pgmq/v1.13.1/pgmq-extension/sql/pgmq.sql -o /usr/share/postgresql/17/extension/pgmq--1.13.1.sql
    ;;
  general)
    apt-get install -y --no-install-recommends postgresql-17-pgvector \
      postgresql-17-postgis-3 postgresql-17-partman postgresql-17-pgaudit \
      postgresql-17-hypopg postgresql-17-repack postgresql-17-pg-ivm \
      'postgresql-17-age=1.7.0-2PIGSTY~trixie' \
      'postgresql-17-pg-net=0.20.5-1PIGSTY~trixie'
    # Match the upstream release artifacts used by the original real acceptance.
    arch="$(dpkg --print-architecture)"
    curl --retry 3 -fsSL "https://github.com/timescale/pgvectorscale/releases/download/0.9.1/pgvectorscale-0.9.1-pg17-${arch}.zip" -o /tmp/vectorscale.zip
    unzip -o /tmp/vectorscale.zip -d /tmp/vectorscale
    mapfile -t packages < <(find /tmp/vectorscale -name '*.deb' ! -name '*-dbgsym_*' -type f)
    test "${#packages[@]}" -eq 1
    curl --retry 3 -fsSL "https://github.com/tensorchord/VectorChord/releases/download/1.1.1/postgresql-17-vchord_1.1.1-1_${arch}.deb" -o /tmp/vchord.deb
    case "$arch" in arm64) release_arch=arm64 ;; amd64) release_arch=amd64 ;; *) exit 1 ;; esac
    curl --retry 3 -fsSL "https://github.com/supabase/pg_jsonschema/releases/download/v0.3.4/pg_jsonschema-v0.3.4-pg17-${release_arch}-linux-gnu.deb" -o /tmp/jsonschema.deb
    apt-get install -y "${packages[0]}" /tmp/vchord.deb /tmp/jsonschema.deb
    ;;
  timescale) apt-get install -y --no-install-recommends 'postgresql-17-timescaledb-tsl=2.31.0-1PGSTY~trixie' ;;
  citus) apt-get install -y --no-install-recommends 'postgresql-17-citus=14.3.0-9PGSTY~trixie' ;;
  duckdb) apt-get install -y --no-install-recommends 'postgresql-17-pg-duckdb=1.1.1-1PIGSTY~trixie' ;;
  *) echo "Unknown fixture profile: $profile" >&2; exit 1 ;;
esac
