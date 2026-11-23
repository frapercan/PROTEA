#!/bin/bash
# One configuration of the embedding bank (ADR-D62) on ONE card, with the tools shipped as
# code only by send_builder_code.sh and the model's weights already in HF_HOME, fetched on
# the login node at a pinned revision. Nothing is downloaded here: the hub is offline.
#
#   embed    FIRST LAST TOKEN_BUDGET  embeds the shards FIRST..LAST not done yet with the
#            plugin's padded pass, and stops before a shard it could not finish by the
#            job's limit (exit 4: resubmit the same range, it resumes).
#   compare  SAMPLE TOKEN_BUDGET      embeds SAMPLE rows spread over the length order one
#            sequence per pass and padded, and records the gap and both speeds.
#   manifest                          writes bank-<config>.json (exit 3 when refused).
#
# Resources go on the sbatch line, so one script serves a card and a CPU:
#   sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 8 --mem=64G embedding_bank.sh ...
#   sbatch -c 2 --mem=8G embedding_bank.sh ... manifest
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR BANK_DIR HF_HOME REVISION CONFIG_ID MODE [MODE ARGUMENTS]
#SBATCH -J embedding-bank
#SBATCH -t 02:00:00
#SBATCH -o embedding-bank-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; BANK=${3:?bank dir}; HF=${4:?hf home}
REV=${5:?revision}; CONFIG=${6:?config id}; MODE=${7:?mode}
shift 7
export PYTHONPATH=$CODE HF_HOME=$HF HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
log() { echo "$(date -Is) $*"; }
[ -s "$BANK/prepare.json" ] || { log "MISSING $BANK/prepare.json"; exit 1; }
[ -s "$BANK/recipes.json" ] || { log "MISSING $BANK/recipes.json"; exit 1; }
RUN=${SLURM_JOB_ID:-$$}
# The pass stops starting shards past this many seconds, leaving the tail for the records.
STOP_AFTER=${STOP_AFTER:-6600}
bank() { "$PY" -m protea.core.embedding_bank.build "$@"; }
log "start $MODE, config $CONFIG, code $REV on $(hostname) ${CUDA_VISIBLE_DEVICES:+card $CUDA_VISIBLE_DEVICES}"
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader 2>/dev/null || true
t=$(date +%s)
set +e
case "$MODE" in
  embed)
    bank embed --bank "$BANK" --config "$CONFIG" --first "${1:?first}" --last "${2:?last}" \
      --token-budget "${3:?token budget}" --run-id "$RUN" --stop-after "$STOP_AFTER"; rc=$? ;;
  compare)
    bank compare --bank "$BANK" --config "$CONFIG" --sample "${1:?sample}" \
      --token-budget "${2:?token budget}" --run-id "$RUN"; rc=$? ;;
  manifest)
    bank manifest --bank "$BANK" --config "$CONFIG" --revision "$REV"; rc=$? ;;
  *) log "REFUSED: unknown mode $MODE"; rc=2 ;;
esac
set -e
log "$MODE exit $rc in $(( $(date +%s) - t )) s (0 done, 2 refused, 3 manifest refused, 4 stopped for time)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "BANK_${MODE^^}_EXIT $rc"
exit "$rc"
