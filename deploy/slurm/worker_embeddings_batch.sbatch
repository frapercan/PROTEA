#!/usr/bin/env bash
#SBATCH --job-name=protea-embed-batch
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --output=logs/protea-embed-batch-%j.out
#SBATCH --error=logs/protea-embed-batch-%j.err
#
# PROTEA embeddings batch worker (mirrors docker-compose service
# `worker-embeddings-batch`). Runs `compute_embeddings_batch` payloads
# off `protea.embeddings.batch` on a GPU node.
#
# Submit with site overrides, for example:
#
#   sbatch \
#     --export=ALL,PROTEA_REPO_DIR=/lustre/protea/PROTEA,\
#PROTEA_VENV=/lustre/protea/.venv,\
#PROTEA_DB_URL=postgresql+psycopg://protea:protea@head:5432/protea,\
#PROTEA_AMQP_URL=amqp://guest:guest@head:5672/ \
#     deploy/slurm/worker_embeddings_batch.sbatch

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

exec python scripts/worker.py \
    --queue protea.embeddings.batch \
    --log-format "${PROTEA_LOG_FORMAT}"
