#!/usr/bin/env bash
#SBATCH --job-name=protea-predict-batch
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-predict-batch-%j.out
#SBATCH --error=logs/protea-predict-batch-%j.err
#
# PROTEA predict-go-terms batch worker (mirrors docker-compose service
# `worker-predictions-batch`). Consumes `predict_go_terms_batch`
# operation messages off `protea.predictions.batch` on a GPU node.
#
# Pre-warm of the taxonomy DB happens inside scripts/worker.py.

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.predictions.batch \
    --log-format "${PROTEA_LOG_FORMAT}"
