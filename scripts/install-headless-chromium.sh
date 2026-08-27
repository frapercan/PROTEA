#!/usr/bin/env bash
# Put a headless browser where Playwright looks, on a distro it refuses to serve.
#
# WHY THIS EXISTS. `npx playwright install chromium` fails on this host with
# "Playwright does not support chromium on ubuntu26.04-x64". The refusal is a
# lookup and not a capability: the registry maps each distro to a download URL,
# there is no ubuntu26.04 row, and the row for ubuntu24.04 points at the generic
# Chrome for Testing linux64 build. The binary Playwright would install on the
# supported release is byte-for-byte the one this fetches.
#
# Without a browser nobody can look at a rendered page, and a surface that is
# only ever inspected as HTML hides exactly the class of defect that costs most:
# on 2026-08-27 the whole application served unstyled markup for an hour, every
# process up, every status code 200, and curl could not tell.
set -euo pipefail

WEB="$(cd "$(dirname "${BASH_SOURCE[0]}")/../apps/web" && pwd)"
REV="$(node -e 'const b=require("'"$WEB"'/node_modules/playwright-core/browsers.json");
  process.stdout.write(b.browsers.find(x=>x.name==="chromium-headless-shell").revision)')"
VER="$(node -e 'const b=require("'"$WEB"'/node_modules/playwright-core/browsers.json");
  process.stdout.write(b.browsers.find(x=>x.name==="chromium-headless-shell").browserVersion)')"

DEST="${PLAYWRIGHT_BROWSERS_PATH:-$HOME/.cache/ms-playwright}/chromium_headless_shell-${REV}"
BIN="$DEST/chrome-headless-shell-linux64/chrome-headless-shell"

if [[ -x "$BIN" ]]; then
  echo "already installed: $("$BIN" --version)"
  exit 0
fi

URL="https://storage.googleapis.com/chrome-for-testing-public/${VER}/linux64/chrome-headless-shell-linux64.zip"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "fetching Chrome for Testing ${VER} for playwright revision ${REV}"
curl -fsSL --max-time 600 -o "$TMP/shell.zip" "$URL"
mkdir -p "$DEST"
rm -rf "$DEST/chrome-headless-shell-linux64"
unzip -q "$TMP/shell.zip" -d "$DEST"
chmod +x "$BIN"

# Assert rather than announce. An unpacked archive that produced no runnable
# binary looks the same as a working install until something tries to launch it.
"$BIN" --version
