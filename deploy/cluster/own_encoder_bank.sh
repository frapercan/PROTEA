#!/bin/bash
# The own segment encoder (jepa-segments) and its embedding bank, with the tools shipped as
# code only by send_builder_code.sh. The input is a complete lexicon bank's cut over the
# population an embedding bank prepared in BANK_DIR (ADR-D62, ADR-D63); nothing is
# downloaded and the encoder never reads a label.
#
#   train   MEMBERS PLAN_MINUTES WALL_MINUTES  trains the encoder on the cut of the member
#           md5s MEMBERS lists (one per line), in ENCODER_DIR: a PLAN_MINUTES plan of
#           training time, this job stopping after WALL_MINUTES (exit 4: submit it again
#           and it resumes from state.pt). It writes encoder.npz and own-encoder.json.
#   sample  MEMBERS MINUTES                    the measured first job: MINUTES of training
#           on the same data in ENCODER_DIR/sample-<job>, the held-out loss every minute,
#           the rate and the card's peak; its record says "measurement" and no bank reads it.
#   vectors ENCODER_SHA256 FIRST LAST          the vectors of the bank shards FIRST..LAST
#           not done yet, with the encoder in ENCODER_DIR, which must hash to
#           ENCODER_SHA256; stops before a shard it could not finish by the job's limit
#           (exit 4). Once no shard of the bank is missing it writes bank-<config>.json
#           (exit 3 when refused). One shard (FIRST = LAST) is the measured first job.
#
# Resources go on the sbatch line; the training's memory from the sample's measured peak
# (the whole lexico-93k cut, 9.6 million segments, loads in 1.3 GB on the desktop's CPU):
#   sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 16 --mem=16G -t 00:15:00 \
#     own_encoder_bank.sh ... sample MEMBERS 5
#   sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 16 --mem=PEAK_TIMES_1_5 \
#     -t 01:15:00 own_encoder_bank.sh ... train MEMBERS 55 65
#
# Positional arguments (a cluster that cancels jobs submitted with --export takes them
# this way):
#   PYTHON CODE_DIR BANK_DIR LEXICON ENCODER_DIR REVISION MODE [MODE ARGUMENTS]
#SBATCH -J own-encoder-bank
#SBATCH -t 01:15:00
#SBATCH -o own-encoder-bank-%j.log
set -euo pipefail
PY=${1:?python}; CODE=${2:?code dir}; BANK=${3:?bank dir}; LEX=${4:?lexicon name}
ENC=${5:?encoder dir}; REV=${6:?revision}; MODE=${7:?mode}
shift 7
export PYTHONPATH=$CODE
log() { echo "$(date -Is) $*"; }
[ -s "$BANK/prepare.json" ] || { log "MISSING $BANK/prepare.json"; exit 1; }
[ -s "$BANK/lexicon/$LEX/lexicon-bank.json" ] || { log "MISSING the lexicon bank $LEX"; exit 1; }
RUN=${SLURM_JOB_ID:-$$}
# The vectors pass stops starting shards past this many seconds, leaving the tail for the records.
STOP_AFTER=${STOP_AFTER:-3300}
train() { "$PY" -m protea.core.own_encoder.train "$@"; }
vectors() { "$PY" -m protea.core.own_encoder.vectors "$@"; }
log "start $MODE, lexicon $LEX, encoder $ENC, code $REV on $(hostname)"
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader 2>/dev/null || true
t=$(date +%s)
set +e
case "$MODE" in
  train)
    MEMBERS=${1:?members}
    [ -s "$MEMBERS" ] || { log "MISSING $MEMBERS"; exit 1; }
    train --bank "$BANK" --lexicon "$LEX" --members "$MEMBERS" --out "$ENC" \
      --plan-minutes "${2:?plan minutes}" --wall-minutes "${3:?wall minutes}" \
      --revision "$REV" --run-id "$RUN"; rc=$? ;;
  sample)
    MEMBERS=${1:?members}
    [ -s "$MEMBERS" ] || { log "MISSING $MEMBERS"; exit 1; }
    train --bank "$BANK" --lexicon "$LEX" --members "$MEMBERS" --out "$ENC/sample-$RUN" \
      --plan-minutes "${2:?minutes}" --wall-minutes "${2:?minutes}" --eval-minutes 1 \
      --measurement --revision "$REV" --run-id "$RUN"; rc=$? ;;
  vectors)
    vectors --bank "$BANK" --lexicon "$LEX" --encoder "$ENC" --encoder-sha256 "${1:?encoder sha256}" \
      --first "${2:?first}" --last "${3:?last}" --stop-after "$STOP_AFTER" \
      --revision "$REV" --run-id "$RUN"; rc=$? ;;
  *) log "REFUSED: unknown mode $MODE"; rc=2 ;;
esac
set -e
log "$MODE exit $rc in $(( $(date +%s) - t )) s (0 done, 2 refused, 3 manifest refused, 4 stopped for time)"
log "peak memory $(sstat -j "${SLURM_JOB_ID:-0}.batch" --format=MaxRSS -n 2>/dev/null | tr -d ' ' || echo unknown)"
log "OWN_ENCODER_${MODE^^}_EXIT $rc"
exit "$rc"
