#!/usr/bin/env bash
# install.sh -- install the PROTEA user units. ENABLE ONLY. NEVER START.
#
# Running this while the current hand-launched stack is live is SAFE: it copies
# unit files, enables them for the next boot, and stops. It does not start,
# restart, reload or signal a single PROTEA process. The multi-hour GPU job
# (protea.embeddings.batch) is not touched.
#
# Reverse with ./uninstall.sh.
set -euo pipefail

SRC="/home/bioxaxi2/Thesis-laptop/PROTEA/deploy/systemd"
DST="$HOME/.config/systemd/user"

UNITS=(
  protea.target
  protea-wait-infra.service
  protea-api.service
  protea-frontend.service
  protea-ngrok.service
  'protea-worker@.service'
)

# The 14 queues currently served, one instance each. protea.jobs runs 4
# consumers today (124785 + 718021/718026/718032); systemd models that as four
# DISTINCT instance names, because a template refuses duplicate instances.
QUEUES=(
  protea.ping
  protea.jobs
  protea.training
  protea.embeddings
  protea.embeddings.batch
  protea.embeddings.write
  protea.predictions
  protea.predictions.batch
  protea.predictions.write
  protea.evaluations
  reaper
)

echo "==> lingering"
if [[ "$(loginctl show-user "$USER" -p Linger --value)" == "yes" ]]; then
  echo "    already enabled"
else
  echo "    Linger=no. Without it, user@$(id -u).service starts only at"
  echo "    GRAPHICAL LOGIN (verified: boot 17:36:56, user manager 17:39:10)"
  echo "    and GDM autologin is disabled -- so nothing would come back until"
  echo "    a human types a password. Enabling it (needs polkit/sudo):"
  echo "        sudo loginctl enable-linger $USER"
  echo "    Re-run this script afterwards."
fi

echo "==> installing units into $DST"
mkdir -p "$DST"
for u in "${UNITS[@]}"; do
  install -m 0644 "$SRC/$u" "$DST/$u"
  echo "    $u"
done

echo "==> daemon-reload (does not start anything)"
systemctl --user daemon-reload

echo "==> enable (NOT --now)"
for u in protea.target protea-wait-infra.service protea-api.service \
         protea-frontend.service protea-ngrok.service; do
  systemctl --user enable "$u" >/dev/null
  echo "    enabled $u"
done
for q in "${QUEUES[@]}"; do
  systemctl --user enable "protea-worker@${q}.service" >/dev/null
  echo "    enabled protea-worker@${q}.service"
done

cat <<'EOF'

==> DONE. Nothing was started. The live stack is untouched.
    Units take effect at the NEXT REBOOT.

    Verify without side effects:
        systemctl --user list-unit-files 'protea*'
        systemd-analyze --user verify ~/.config/systemd/user/protea-api.service

    Do NOT run `systemctl --user start protea.target` while the hand-launched
    stack is live: the API would fail to bind :8000 and every worker instance
    would be refused by its ExecStartPre guard. Harmless, but noisy. The
    correct transition is a reboot.
EOF
