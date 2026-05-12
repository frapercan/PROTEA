#!/usr/bin/env bash
#SBATCH --job-name=protea-embed
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-embed-%j.out
#SBATCH --error=logs/protea-embed-%j.err
#
# PROTEA embeddings coordinator worker (mirrors `worker-embeddings`).
# Consumes `protea.embeddings`: this queue holds the per-job
# `compute_embeddings` orchestration messages that fan out batches to
# the GPU workers. No GPU required.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.embeddings \
    --log-format "${PROTEA_LOG_FORMAT}"
