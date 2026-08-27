#!/usr/bin/env bash
# uninstall.sh -- full reversal. Disables and removes the PROTEA user units.
# Does NOT stop running processes: --no-block is not needed because we never
# call stop. If the units are live and you want them down, do that explicitly.
set -euo pipefail

DST="$HOME/.config/systemd/user"

mapfile -t found < <(cd "$DST" 2>/dev/null && ls protea*.service protea.target 2>/dev/null || true)
for u in "${found[@]}"; do
  systemctl --user disable "$u" >/dev/null 2>&1 || true
  rm -f "$DST/$u"
  echo "removed $u"
done
systemctl --user daemon-reload
echo "done. (linger, if you enabled it, is separate: sudo loginctl disable-linger $USER)"
