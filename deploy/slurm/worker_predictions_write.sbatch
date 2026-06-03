#!/usr/bin/env bash
#SBATCH --job-name=protea-predict-write
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-predict-write-%j.out
#SBATCH --error=logs/protea-predict-write-%j.err
#
# PROTEA predictions DB writer (mirrors `worker-predictions-write`).
# Consumes `protea.predictions.write`: serialised DB inserts of GO term
# predictions produced by the GPU prediction workers.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.predictions.write \
    --log-format "${PROTEA_LOG_FORMAT}"
