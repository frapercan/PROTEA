#!/usr/bin/env bash
#SBATCH --job-name=protea-jobs
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-jobs-%j.out
#SBATCH --error=logs/protea-jobs-%j.err
#
# Generic PROTEA job worker (mirrors `worker-jobs`). Consumes the
# default `protea.jobs` queue, dispatching whichever operation is on
# the message. No GPU required; ingest / fetch / load operations run
# here.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.jobs \
    --log-format "${PROTEA_LOG_FORMAT}"
