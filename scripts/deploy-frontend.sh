#!/usr/bin/env bash
# Build and deploy the frontend WITHOUT risking the public site.
#
# `next build` deletes and recreates .next, which is the directory the
# running server stands in, and there is no atomic swap anywhere in this
# repository (scripts/deploy-check.sh:4-11 records the 2026-08-20 outage
# that taught us). A build that FAILS therefore leaves no .next at all
# and the unit cannot start: on 2026-10-07 a two-minute network blip made
# next/font/google unreachable, the build died, and protea.ngrok.app
# answered 502 for ten minutes.
#
# So: keep the serving build, replace it only once a new one exists, and
# put the old one back if anything goes wrong.
set -uo pipefail

WEB="$(cd "$(dirname "${BASH_SOURCE[0]}")/../apps/web" && pwd)"
BACKUP="${TMPDIR:-/tmp}/protea-next-rollback"
cd "$WEB"

if [[ ! -f .next/standalone/server.js ]]; then
  echo "no serving build to protect; building from nothing"
else
  echo "keeping the serving build in $BACKUP"
  rm -rf "$BACKUP"
  cp -a .next "$BACKUP"
fi

if npm run build; then
  echo "build ok; restarting"
  systemctl --user restart protea-frontend.service
  sleep 6
  for attempt in 1 2 3 4 5; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 http://127.0.0.1:3000/en/ || true)
    [[ "$code" == "200" ]] && { echo "serving 200, build $(cat .next/BUILD_ID)"; rm -rf "$BACKUP"; exit 0; }
    sleep 4
  done
  echo "built but NOT serving (last code: ${code:-none}); rolling back"
else
  echo "build FAILED; rolling back"
fi

if [[ -d "$BACKUP" ]]; then
  rm -rf .next && cp -a "$BACKUP" .next
  systemctl --user restart protea-frontend.service
  sleep 6
  echo "rolled back to $(cat .next/BUILD_ID 2>/dev/null); serving $(curl -s -o /dev/null -w '%{http_code}' --max-time 20 http://127.0.0.1:3000/en/)"
fi
exit 1
