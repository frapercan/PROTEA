#!/usr/bin/env bash
# protea-dev-clock-gate.sh UNIT
#
# ExecStartPre for the dev units: hold the unit back until the system clock
# is synchronised, for a bounded time, and NEVER refuse to start it.
#
# WHY. This machine dual-boots, and Windows writes LOCAL time to the hardware
# clock. After a Windows session the kernel reads that clock as UTC, so Linux
# comes up hours off until chronyd steps it. Measured on 2026-08-29: a worker
# consumed a job and wrote three events, and three seconds later chronyd
# stepped the clock by -7199.5 s; those rows are stamped two hours in the
# future and nothing downstream can tell them from real ones. Job leases last
# 120 s and the reaper acts on expired leases, so a step of hours can also
# reap a healthy job. The production worker wrapper on this machine carries
# the same gate; this is its logic, for the dev units.
#
# WHY NOT After=time-sync.target. These are USER units, and a user manager
# has no time-sync.target: `systemctl --user show time-sync.target -p
# LoadState` says not-found on this machine. The ordering would order against
# nothing while reading like a gate.
#
# IT NEVER EXITS NON-ZERO. A dev unit that stays down because NTP is broken
# is a worse failure than a few events with a skewed timestamp, so when the
# budget runs out the unit starts anyway and the log says so. Everything that
# could fail runs in a subshell whose status is discarded.
#
# Budget: PROTEA_DEV_CLOCK_WAIT seconds, default 120 (about six times the
# measured 12 to 19 s exposure), capped at 600, 0 disables the wait. The
# unit's TimeoutStartSec must stay above the cap.
#
# READ-ONLY apart from one marker file under $XDG_RUNTIME_DIR: reads
# timedatectl and /proc/uptime, starts nothing, kills nothing.

UNIT="${1:-unknown}"
UNIT="${UNIT//\//_}"

log() { echo "protea-dev-clock-gate: ${UNIT}: $*" >&2; }

synchronised() {
  # Only the literal "yes" counts: timedatectl prints yes, no, or nothing at
  # all for an unknown property while exiting 0. Each probe is bounded, so a
  # wedged system bus cannot hold the unit for sd-bus's own patience.
  # NTPSynchronized is the kernel's STA_UNSYNC bit through adjtimex, so it is
  # the same clock the unit will stamp, whichever NTP client runs.
  local answer
  answer="$(timeout 5 timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
  [[ "${answer}" == "yes" ]]
}

uptime_seconds() {
  # CLOCK_BOOTTIME, never the wall clock: the wall clock is the thing under
  # test, and a deadline read from it would gain two hours of budget at the
  # instant chronyd steps it back. Bash's $SECONDS has the same defect.
  local up rest
  read -r up rest < /proc/uptime || return 1
  echo "${up%%.*}"
}

gate() {
  local budget="${PROTEA_DEV_CLOCK_WAIT:-120}"
  # Validated and forced to base ten: the value comes from the environment,
  # an arithmetic context reads "0120" as octal and "09" as an error.
  if [[ ! "${budget}" =~ ^[0-9]+$ ]]; then
    log "PROTEA_DEV_CLOCK_WAIT wants whole seconds, got '${budget}'; using 120"
    budget=120
  fi
  budget=$(( 10#${budget} ))
  if (( budget > 600 )); then
    log "PROTEA_DEV_CLOCK_WAIT=${budget} is above the 600 s cap; using 600"
    budget=600
  fi

  # The skew is created once per boot, when the kernel reads the hardware
  # clock, so a unit that already sat out the whole budget on this boot has
  # nothing to learn from sitting it out again on every restart. The marker
  # lives on tmpfs (once per boot for free), is scoped to the unit so one
  # instance giving up cannot disarm the others, and only ever means less
  # waiting.
  local marker="${XDG_RUNTIME_DIR:-/tmp}/protea-dev-clock-gate.${UNIT}.unsynced"
  local note=""
  if [[ -e "${marker}" ]]; then
    budget=0
    note=" (this unit already waited the full budget out on this boot)"
  fi

  if ! command -v timedatectl >/dev/null 2>&1; then
    log "WARNING no timedatectl, so the clock cannot be checked; starting anyway"
    log "clock=unchecked"
    return 0
  fi
  if ! command -v timeout >/dev/null 2>&1; then
    log "WARNING no timeout(1), so a probe could not be bounded; starting anyway"
    log "clock=unchecked"
    return 0
  fi

  # Both baselines in the same breath, before the first probe: the step is
  # the difference between how far each clock moved.
  local up0 wall0
  if ! up0="$(uptime_seconds)"; then
    log "WARNING cannot read /proc/uptime, so a wait could not be bounded; starting anyway"
    log "clock=unchecked"
    return 0
  fi
  wall0="$(date +%s 2>/dev/null || true)"

  local verdict=unsynced held=no polls=0 now
  if synchronised; then
    verdict=ok
  else
    if (( budget > 0 )); then
      held=yes
      log "clock NOT synchronised (up ${up0}s, reads $(date -Is)); holding the unit for up to ${budget}s rather than stamp events with a clock that is about to move"
    fi
    # The poll count is the loop condition, so the loop ends even if the
    # elapsed arithmetic is ever wrong; a zero budget runs it zero times.
    while (( polls < budget )); do
      sleep 1
      polls=$(( polls + 1 ))
      if synchronised; then
        verdict=ok
        break
      fi
      if ! now="$(uptime_seconds)"; then
        log "WARNING /proc/uptime went unreadable mid-wait; ending the wait"
        break
      fi
      if (( now - up0 >= budget )); then
        break
      fi
    done
  fi

  # How far the wall clock moved beyond the wait itself is the size of
  # chronyd's step; hwclock is not installed here, so this is the only measure.
  local waited=0 step=0 wall1
  if now="$(uptime_seconds)"; then
    waited=$(( now - up0 ))
  fi
  wall1="$(date +%s 2>/dev/null || true)"
  if [[ "${wall0}" =~ ^[0-9]+$ && "${wall1}" =~ ^[0-9]+$ ]]; then
    step=$(( (wall1 - wall0) - waited ))
  fi

  if [[ "${verdict}" == "ok" ]]; then
    rm -f "${marker}" 2>/dev/null || true
    if [[ "${held}" == "yes" ]]; then
      log "clock synchronised after ${waited}s of waiting"
    fi
  else
    touch "${marker}" 2>/dev/null || true
    log "WARNING clock still unsynchronised after ${waited}s${note}; starting anyway. Check 'timedatectl status' and chronyd."
  fi

  if (( step <= -2 || step >= 2 )); then
    log "the system clock STEPPED by ${step}s during this wait; lines above carry the old clock"
    # Rounded to the NEAREST hour: a two-hour step minus genuine drift arrives
    # as 7199 s, and truncation would call it one hour.
    local abs="${step#-}" hours slop
    hours=$(( (abs + 1800) / 3600 ))
    slop=$(( abs - hours * 3600 ))
    slop="${slop#-}"
    if (( hours >= 1 && hours <= 14 && slop <= 60 )); then
      log "a step of ${hours}h is the dual boot: Windows leaves LOCAL time in the hardware clock and Linux reads it as UTC"
    fi
  fi

  # One token a later reader can grep for: ok, unsynced or unchecked.
  log "clock=${verdict}"
  return 0
}

( set -u; gate )
exit 0
