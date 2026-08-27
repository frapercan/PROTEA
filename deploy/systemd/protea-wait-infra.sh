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
set -euo pipefail

DEADLINE=${PROTEA_WAIT_DEADLINE:-180}
declare -a PORTS=("127.0.0.1:5432" "127.0.0.1:5672")

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
echo "protea-wait-infra: infra ports ready"
