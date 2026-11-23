#!/bin/bash
# Disk gate for the desktop development stack (ExecStartPre of every dev unit).
#
# Development shares the system disk with production on this machine: the
# author decided on 2026-11-15 to work with the disk each machine has, with no
# new disk and no copies. So a dev unit must stop before the dev data can fill
# the disk production runs on. This gate refuses to start when the file system
# that holds the dev data root has less than PROTEA_DEV_MIN_FREE_GB free
# (default 100), and when the root is missing.
#
# Refusals exit 78 (EX_CONFIG), the status dev units do not restart on. Units
# also carry StartLimitBurst, for the ones whose restart policy does not read
# an ExecStartPre status.
#
# Usage: protea-dev-disk-gate.sh <dev data root> [unit name]
set -uo pipefail

root="${1:-}"
unit="${2:-dev unit}"
floor="${PROTEA_DEV_MIN_FREE_GB:-100}"

if [[ -z "$root" ]]; then
    echo "$unit refused: no dev data root given" >&2
    exit 78
fi
if [[ ! -d "$root" ]]; then
    echo "$unit refused: the dev data root $root does not exist" >&2
    exit 78
fi
if ! [[ "$floor" =~ ^[0-9]+$ ]]; then
    echo "$unit refused: PROTEA_DEV_MIN_FREE_GB must be a whole number of GB, got '$floor'" >&2
    exit 78
fi

free_kib="$(df --output=avail -k -- "$root" 2>/dev/null | tail -n 1 | tr -d ' ')"
if ! [[ "$free_kib" =~ ^[0-9]+$ ]]; then
    echo "$unit refused: cannot read the free space of the file system of $root" >&2
    exit 78
fi
free_gb=$(( free_kib / 1024 / 1024 ))

if (( free_gb < floor )); then
    echo "$unit refused: ${free_gb} GB free on the file system of $root, below the floor of ${floor} GB" >&2
    exit 78
fi
echo "$unit: ${free_gb} GB free on the file system of $root (floor ${floor} GB), disk=ok"
