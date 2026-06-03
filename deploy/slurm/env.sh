# shellcheck shell=bash
# deploy/slurm/env.sh
#
# Shared environment defaults for PROTEA SLURM submissions (deployment
# mode B per D25). Source this file from every sbatch script so workers
# get a consistent picture of where Postgres, RabbitMQ, MinIO, and the
# code checkout live.
#
# Override any value by exporting it BEFORE sourcing this file, or by
# adding `--export=PROTEA_DB_URL=...,PROTEA_AMQP_URL=...` on the sbatch
# command line. We never overwrite a variable that is already set.
#
# Required on every node:
#   PROTEA_REPO_DIR   absolute path to a PROTEA checkout shared across
#                     compute nodes (e.g. /lustre/protea/PROTEA)
#   PROTEA_VENV       path to the python venv with PROTEA installed
#                     (e.g. /lustre/protea/.venv)
#   PROTEA_DB_URL     SQLAlchemy URL of the Postgres reachable from
#                     compute nodes (typically the same head-node host)
#   PROTEA_AMQP_URL   URL of the RabbitMQ broker reachable from
#                     compute nodes
#
# Optional storage block: set PROTEA_STORAGE_BACKEND=minio plus the
# MinIO credentials below, or leave PROTEA_STORAGE_BACKEND=local and
# point PROTEA_STORAGE_ROOT at a shared filesystem.

set -euo pipefail

: "${PROTEA_REPO_DIR:?PROTEA_REPO_DIR must be set (path to the PROTEA checkout)}"
: "${PROTEA_VENV:?PROTEA_VENV must be set (path to the python venv)}"
: "${PROTEA_DB_URL:?PROTEA_DB_URL must be set (Postgres SQLAlchemy URL)}"
: "${PROTEA_AMQP_URL:?PROTEA_AMQP_URL must be set (RabbitMQ AMQP URL)}"

export PROTEA_REPO_DIR
export PROTEA_VENV
export PROTEA_DB_URL
export PROTEA_AMQP_URL

# Storage backend defaults to local; flip to minio in your site env.
export PROTEA_STORAGE_BACKEND="${PROTEA_STORAGE_BACKEND:-local}"
export PROTEA_STORAGE_ROOT="${PROTEA_STORAGE_ROOT:-${PROTEA_REPO_DIR}/storage}"

# MinIO knobs (only consulted when STORAGE_BACKEND=minio).
export PROTEA_MINIO_ENDPOINT="${PROTEA_MINIO_ENDPOINT:-}"
export PROTEA_MINIO_BUCKET="${PROTEA_MINIO_BUCKET:-protea}"
export PROTEA_MINIO_ACCESS_KEY="${PROTEA_MINIO_ACCESS_KEY:-}"
export PROTEA_MINIO_SECRET_KEY="${PROTEA_MINIO_SECRET_KEY:-}"
export PROTEA_MINIO_SECURE="${PROTEA_MINIO_SECURE:-0}"

# Observability: where to push OTLP traces (mirrors T5.1a).
export PROTEA_OTLP_ENDPOINT="${PROTEA_OTLP_ENDPOINT:-}"

# Worker log format defaults to json so SLURM stdout stays grep-friendly.
export PROTEA_LOG_FORMAT="${PROTEA_LOG_FORMAT:-json}"

# Activate the venv if it has not been activated yet.
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    # shellcheck disable=SC1091
    source "${PROTEA_VENV}/bin/activate"
fi

cd "${PROTEA_REPO_DIR}"
