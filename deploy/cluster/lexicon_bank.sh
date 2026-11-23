#!/bin/bash
# One lexicon's bank over the photos' population (ADR-D63) on ONE card, with the tools shipped
# as code only by send_builder_code.sh. The population is the one an embedding bank prepared
# (ADR-D62) in BANK_DIR, and the lexicon's weights are placed beforehand: they must hash to
# LEXICON_SHA256 before anything is read. Nothing is downloaded.
#
#   cut      FIRST LAST    the shards FIRST..LAST not done yet: segments and columns on the
#                          best path; stops before a shard it could not finish by the job's
#                          limit (exit 4: resubmit the same range, it resumes)
#   mass     FIRST LAST    the same for the theme mass
#   manifest PARITY_JSON   writes lexicon/NAME/lexicon-bank.json (exit 3 when refused)
#
# Triton builds a small launcher with the interpreter's headers the first time a kernel runs;
# the cluster's uv-managed Python ships them. Resources go on the sbatch line:
#   sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 8 --mem=96G lexicon_bank.sh ...
#   sbatch -c 2 --mem=8G lexicon_bank.sh ... manifest PARITY_JSON
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR BANK_DIR LEXICON_PATH LEXICON_SHA256 NAME REVISION MODE [MODE ARGUMENTS]
#SBATCH -J lexicon-bank
#SBATCH -t 02:00:00
#SBATCH -o lexicon-bank-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; BANK=${3:?bank dir}; LEX=${4:?lexicon path}
LEX_SHA=${5:?lexicon sha256}; NAME=${6:?lexicon name}; REV=${7:?revision}; MODE=${8:?mode}
shift 8
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[ -s "$BANK/prepare.json" ] || { log "MISSING $BANK/prepare.json"; exit 1; }
[ -s "$LEX" ] || { log "MISSING $LEX"; exit 1; }
RUN=${SLURM_JOB_ID:-$$}
# The pass stops starting shards past this many seconds, leaving the tail for the records.
STOP_AFTER=${STOP_AFTER:-6600}
lexbank() { "$PY" -m protea.core.lexicon_bank.build "$@"; }
log "start $MODE, lexicon $NAME, code $REV on $(hostname)"
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader 2>/dev/null || true
t=$(date +%s)
set +e
case "$MODE" in
  cut|mass)
    lexbank "$MODE" --bank "$BANK" --name "$NAME" --lexicon "$LEX" --lexicon-sha256 "$LEX_SHA" \
      --first "${1:?first}" --last "${2:?last}" --run-id "$RUN" --stop-after "$STOP_AFTER"; rc=$? ;;
  manifest)
    lexbank manifest --bank "$BANK" --name "$NAME" --lexicon-sha256 "$LEX_SHA" \
      --parity "${1:?parity json}" --revision "$REV"; rc=$? ;;
  *) log "REFUSED: unknown mode $MODE"; rc=2 ;;
esac
set -e
log "$MODE exit $rc in $(( $(date +%s) - t )) s (0 done, 2 refused, 3 manifest refused, 4 stopped for time)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "LEXICON_${MODE^^}_EXIT $rc"
exit "$rc"
