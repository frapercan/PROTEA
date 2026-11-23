#!/usr/bin/env bash
# Download one public source of a cut photo ON THE CLUSTER'S LOGIN NODE (ADR-D59).
#
# The compute nodes cannot reach the internet, so every source a photo is built from
# is downloaded here first and read by the jobs from the shared scratch. The file is
# fetched as parallel byte ranges (UniProt's and EBI's servers both answer 206), each
# retried and size-checked, then joined in order and checked against its size and,
# when given, its md5 (UniProt publishes one per file in RELEASE.metalink; EBI
# publishes none for GOA). Its sha256 is printed last, to be recorded and checked by
# the job that reads it.
#
# With "nojoin" the parts stay as FILE.part<N> and are not joined: a UniRef archive of
# 190 GB joined in place would need twice its size, and the measuring job streams the
# parts in order and checks the md5 of the stream as it reads.
#
# A GOA release after the mark and a UniProt release at or after the ceiling are
# refused before any byte is fetched (protea.core.marks: GOA 227, UniProt 2025_04).
#
# Run with nohup; one log line per event.
#   fetch_source.sh URL FILE BYTES PARTS [MD5] [join|nojoin]
set -euo pipefail
URL=${1:?url}; FILE=${2:?file}; SIZE=${3:?bytes}; PARTS=${4:?parts}; MD5=${5:-}; JOIN=${6:-join}
log() { echo "$(date -Is) $*"; }
if [[ "$URL" =~ goa_uniprot_all\.gaf\.([0-9]+)\.gz$ ]] && [ "${BASH_REMATCH[1]}" -gt 227 ]; then
  log "REFUSED: GOA ${BASH_REMATCH[1]} is after the mark 227"; exit 2
fi
if [[ "$URL" =~ release-(2025_0[4-9]|2025_1[0-9]|20[3-9][0-9]_[0-9]+|202[6-9]_[0-9]+)/ ]]; then
  log "REFUSED: UniProt ${BASH_REMATCH[1]} is at or after the ceiling 2025_04"; exit 2
fi
mkdir -p "$(dirname "$FILE")"
if [ -s "$FILE" ] && [ "$(stat -c %s "$FILE")" = "$SIZE" ]; then
  log "already here: $FILE"
else
  chunk=$(( (SIZE + PARTS - 1) / PARTS )); pids=(); t0=$(date +%s)
  for i in $(seq 0 $((PARTS - 1))); do
    s=$(( i * chunk )); e=$(( (i + 1) * chunk - 1 )); [ "$e" -ge "$SIZE" ] && e=$(( SIZE - 1 ))
    ( for _ in 1 2 3 4 5; do
        curl -sS --retry 5 --retry-delay 20 -r "$s-$e" -o "$FILE.part$i" "$URL" \
          && [ "$(stat -c %s "$FILE.part$i")" = "$(( e - s + 1 ))" ] && exit 0
        sleep 20
      done; exit 1 ) &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || { log "RANGE FAILED $FILE"; exit 1; }; done
  if [ "$JOIN" = "nojoin" ]; then
    log "downloaded $PARTS parts of $FILE in $(( $(date +%s) - t0 )) s, not joined"
    log "SOURCE_FETCHED $FILE (parts)"
    exit 0
  fi
  for i in $(seq 0 $((PARTS - 1))); do cat "$FILE.part$i"; done > "$FILE"
  rm -f "$FILE".part*
  log "downloaded $FILE in $(( $(date +%s) - t0 )) s"
fi
[ "$(stat -c %s "$FILE")" = "$SIZE" ] || { log "SIZE MISMATCH $FILE"; exit 1; }
if [ -n "$MD5" ]; then
  got=$(md5sum "$FILE" | cut -d' ' -f1)
  [ "$got" = "$MD5" ] || { log "MD5 MISMATCH $FILE $got != $MD5"; exit 1; }
  log "md5 ok $FILE"
fi
log "sha256 $(sha256sum "$FILE" | cut -d' ' -f1) $FILE"
log "SOURCE_FETCHED $FILE"
