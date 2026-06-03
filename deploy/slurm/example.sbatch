#!/usr/bin/env bash
#SBATCH --job-name=protea-env-check
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:05:00
#SBATCH --output=logs/protea-env-check-%j.out
#SBATCH --error=logs/protea-env-check-%j.err
#
# Smoke test: resolve all PROTEA_* env vars expected by the worker
# scripts and print them. Lets a site operator verify that env.sh + the
# sbatch --export contract is wired correctly before launching real
# GPU jobs.
#
# Submit with:
#
#   sbatch \
#     --export=ALL,PROTEA_REPO_DIR=/lustre/protea/PROTEA,\
#PROTEA_VENV=/lustre/protea/.venv,\
#PROTEA_DB_URL=postgresql+psycopg://protea:protea@head:5432/protea,\
#PROTEA_AMQP_URL=amqp://guest:guest@head:5672/ \
#     deploy/slurm/example.sbatch

set -euo pipefail

# shellcheck source=deploy/slurm/env.sh
source "$(dirname "$0")/env.sh"

echo "=== PROTEA SLURM env check ==="
echo "host           : $(hostname)"
echo "PROTEA_REPO_DIR: ${PROTEA_REPO_DIR}"
echo "PROTEA_VENV    : ${PROTEA_VENV}"
echo "PROTEA_DB_URL  : ${PROTEA_DB_URL}"
echo "PROTEA_AMQP_URL: ${PROTEA_AMQP_URL}"
echo "STORAGE        : backend=${PROTEA_STORAGE_BACKEND} root=${PROTEA_STORAGE_ROOT}"
echo "MINIO          : endpoint=${PROTEA_MINIO_ENDPOINT} bucket=${PROTEA_MINIO_BUCKET} secure=${PROTEA_MINIO_SECURE}"
echo "OTLP           : ${PROTEA_OTLP_ENDPOINT:-<unset>}"
echo "LOG_FORMAT     : ${PROTEA_LOG_FORMAT}"
echo "python         : $(command -v python)"
echo "=== importing protea ==="
python -c "import protea; print('protea OK from', protea.__file__)"
