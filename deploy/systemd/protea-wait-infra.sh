#!/usr/bin/env bash
# protea-wait-infra.sh -- READ-ONLY readiness gate for the PROTEA user units.
#
# At boot the infra containers (postgres/rabbitmq/minio) are started by Docker
# under `restart: unless-stopped`, but they are not ready the instant the user
# manager reaches default.target. This script BLOCKS until the TCP ports answer.
#
# It deliberately does NOTHING else. It does not start containers, does not run
# docker, does not touch git, does not install anything. If infra never comes
# up it exits non-zero and systemd retries the unit via Restart=on-failure.
#
# Environment:
#   PROTEA_WAIT_DEADLINE   seconds before giving up, default 180.
#   PROTEA_WAIT_ENDPOINTS  whitespace-separated host:port list, default
#                          "127.0.0.1:5432 127.0.0.1:5672" (the production
#                          Postgres and RabbitMQ). Another stack on the same
#                          machine (deploy/dev) reuses this gate by naming its
#                          own ports here. A malformed entry exits 78
#                          (EX_CONFIG): it is a configuration error, and
#                          waiting would only hide it.
set -euo pipefail

DEADLINE=${PROTEA_WAIT_DEADLINE:-180}
read -r -a PORTS <<< "${PROTEA_WAIT_ENDPOINTS:-127.0.0.1:5432 127.0.0.1:5672}"

if (( ${#PORTS[@]} == 0 )); then
  echo "protea-wait-infra: PROTEA_WAIT_ENDPOINTS names no endpoint" >&2
  exit 78
fi
for hp in "${PORTS[@]}"; do
  port=${hp##*:}
  if [[ ! "${hp}" =~ ^[A-Za-z0-9._-]+:[0-9]{1,5}$ ]] || (( 10#${port} < 1 || 10#${port} > 65535 )); then
    echo "protea-wait-infra: '${hp}' in PROTEA_WAIT_ENDPOINTS is not host:port" >&2
    exit 78
  fi
done

deadline=$(( SECONDS + DEADLINE ))
for hp in "${PORTS[@]}"; do
  host=${hp%:*}; port=${hp##*:}
  until (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "protea-wait-infra: ${hp} not answering after ${DEADLINE}s" >&2
      exit 1
    fi
    sleep 2
  done
  exec 3<&- 2>/dev/null || true
done
echo "protea-wait-infra: infra ports ready (${PORTS[*]})"
