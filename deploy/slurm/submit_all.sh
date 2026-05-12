#!/usr/bin/env bash
# deploy/slurm/submit_all.sh
#
# Convenience wrapper: submit one of each PROTEA worker to SLURM, in
# the same order docker-compose brings them up. Re-run as needed; each
# sbatch is independent and idempotent (SLURM gives every submission
# its own job id).
#
# Required: PROTEA_REPO_DIR, PROTEA_VENV, PROTEA_DB_URL, PROTEA_AMQP_URL
# exported in the caller's shell (they are forwarded via --export=ALL).
#
# Optional flags:
#   --partition-cpu  override CPU partition (default: cpu)
#   --partition-gpu  override GPU partition (default: gpu)
#   --no-gpu         skip the GPU sbatch files (useful for CPU-only
#                    clusters or smoke testing)
#   --dry-run        print sbatch commands without executing them

set -euo pipefail

PARTITION_CPU="cpu"
PARTITION_GPU="gpu"
NO_GPU=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --partition-cpu) PARTITION_CPU="$2"; shift 2 ;;
        --partition-gpu) PARTITION_GPU="$2"; shift 2 ;;
        --no-gpu)        NO_GPU=1; shift ;;
        --dry-run)       DRY_RUN=1; shift ;;
        -h|--help)
            sed -n '3,20p' "$0"
            exit 0
            ;;
        *)
            echo "unknown flag: $1" >&2
            exit 2
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CPU_JOBS=(
    "worker_jobs.sbatch"
    "worker_embeddings.sbatch"
    "worker_embeddings_write.sbatch"
    "worker_predictions_write.sbatch"
    "worker_reaper.sbatch"
)
GPU_JOBS=(
    "worker_embeddings_batch.sbatch"
    "worker_predictions_batch.sbatch"
)

submit() {
    local partition="$1"
    local script="$2"
    local cmd=(sbatch --export=ALL --partition="${partition}" "${SCRIPT_DIR}/${script}")
    if (( DRY_RUN )); then
        printf '%s\n' "${cmd[*]}"
    else
        "${cmd[@]}"
    fi
}

for job in "${CPU_JOBS[@]}"; do
    submit "${PARTITION_CPU}" "${job}"
done

if (( NO_GPU == 0 )); then
    for job in "${GPU_JOBS[@]}"; do
        submit "${PARTITION_GPU}" "${job}"
    done
fi
