#!/usr/bin/env bash
#SBATCH --job-name=protea-embed-write
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-embed-write-%j.out
#SBATCH --error=logs/protea-embed-write-%j.err
#
# PROTEA embeddings DB writer (mirrors `worker-embeddings-write`).
# Consumes `protea.embeddings.write`: serialised DB inserts of
# embedding batches produced by the GPU workers.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.embeddings.write \
    --log-format "${PROTEA_LOG_FORMAT}"
