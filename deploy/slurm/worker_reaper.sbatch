#!/usr/bin/env bash
#SBATCH --job-name=protea-reaper
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M
#SBATCH --time=7-00:00:00
#SBATCH --output=logs/protea-reaper-%j.out
#SBATCH --error=logs/protea-reaper-%j.err
#
# PROTEA stale-job reaper (mirrors `worker-reaper`). No queue: polls
# the DB every 60 s for jobs stuck past the configured timeout / stall
# window and marks them failed so coordinators can finish.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue reaper \
    --log-format "${PROTEA_LOG_FORMAT}"
