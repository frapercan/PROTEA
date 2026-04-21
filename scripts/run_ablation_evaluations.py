#!/usr/bin/env python
"""Phase B launcher — ablation evaluations.

Consumes the TSV manifest produced by ``run_ablation_predictions.py`` and submits
one ``run_cafa_evaluation`` job per (prediction_set × scoring_config) combination.
All 5 preset scoring configs (``embedding_only``, ``embedding_plus_evidence``,
``alignment_weighted``, ``composite``, ``evidence_primary``) are applied to every
prediction set, for a default fan-out of 24 prediction_sets × 5 = 120 jobs.

Usage
-----
    poetry run python scripts/run_ablation_evaluations.py \\
        --manifest results/ablation_predictions_<timestamp>.tsv \\
        --eval-set a73cb77c-9adf-4d55-b61f-c0b1bd05be01 \\
        [--scoring embedding_only,embedding_plus_evidence,...] \\
        [--dry-run] [--no-wait] [--api http://localhost:8000]
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

DEFAULT_SCORINGS = [
    "embedding_only",
    "embedding_plus_evidence",
    "alignment_weighted",
    "composite",
    "evidence_primary",
]


def _load_scoring_map(api: str) -> dict[str, str]:
    r = requests.get(f"{api}/scoring/configs", timeout=30)
    r.raise_for_status()
    return {c["name"]: c["id"] for c in r.json()}


def _load_manifest(path: Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def submit_eval(api: str, eval_set_id: str, pred_set_id: str, scoring_id: str) -> str:
    body = {
        "prediction_set_id": pred_set_id,
        "scoring_config_id": scoring_id,
    }
    r = requests.post(
        f"{api}/annotations/evaluation-sets/{eval_set_id}/run",
        json=body,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["id"]


def wait_for(api: str, job_id: str, poll: float = 15.0, timeout: float = 3600.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        r = requests.get(f"{api}/jobs/{job_id}", timeout=30)
        r.raise_for_status()
        job = r.json()
        if job["status"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return job
        if time.monotonic() > deadline:
            raise TimeoutError(f"{job_id} timed out after {timeout}s")
        time.sleep(poll)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True,
                    help="TSV from run_ablation_predictions.py")
    ap.add_argument("--eval-set", required=True)
    ap.add_argument("--scoring", default=",".join(DEFAULT_SCORINGS))
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--output", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--poll", type=float, default=20.0)
    args = ap.parse_args()

    scoring_names = [s.strip() for s in args.scoring.split(",") if s.strip()]
    scoring_map = _load_scoring_map(args.api)
    missing = [n for n in scoring_names if n not in scoring_map]
    if missing:
        print(f"[err] unknown scoring configs: {missing}", flush=True)
        print(f"      available: {sorted(scoring_map)}", flush=True)
        return 1

    rows = _load_manifest(Path(args.manifest))
    rows = [r for r in rows if r.get("status") == "SUCCEEDED" and r.get("prediction_set_id")]
    if not rows:
        print("[err] manifest has no SUCCEEDED rows with prediction_set_id", flush=True)
        return 1

    total = len(rows) * len(scoring_names)
    print(f"[plan] {len(rows)} prediction set(s) × {len(scoring_names)} scoring = {total} job(s)",
          flush=True)

    if args.dry_run:
        for r in rows:
            for name in scoring_names:
                print(f"  [dry] {r['model']:12s} k={r['k']:>2}  "
                      f"ps={r['prediction_set_id']}  scoring={name}")
        return 0

    submitted: list[dict] = []
    for r in rows:
        for name in scoring_names:
            try:
                job_id = submit_eval(
                    args.api,
                    eval_set_id=args.eval_set,
                    pred_set_id=r["prediction_set_id"],
                    scoring_id=scoring_map[name],
                )
                print(f"[submit] {r['model']:12s} k={r['k']:>2}  scoring={name:24s}  "
                      f"job={job_id}", flush=True)
                submitted.append({
                    "model": r["model"],
                    "k": r["k"],
                    "prediction_set_id": r["prediction_set_id"],
                    "scoring_config": name,
                    "job_id": job_id,
                })
            except requests.HTTPError as exc:
                print(f"[err] {r['model']}/k{r['k']}/{name}: "
                      f"{exc.response.status_code} {exc.response.text}", flush=True)

    out_rows: list[dict] = []
    if args.no_wait:
        for s in submitted:
            out_rows.append({**s, "status": "QUEUED"})
    else:
        for s in submitted:
            print(f"[poll] {s['model']}/k{s['k']}/{s['scoring_config']} "
                  f"({s['job_id']}) …", end=" ", flush=True)
            try:
                job = wait_for(args.api, s["job_id"], poll=args.poll)
                print(job["status"], flush=True)
                out_rows.append({**s, "status": job["status"]})
            except TimeoutError as exc:
                print(f"TIMEOUT {exc}", flush=True)
                out_rows.append({**s, "status": "TIMEOUT"})

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(args.output) if args.output else Path(
        f"results/ablation_evaluations_{stamp}.tsv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["model", "k", "prediction_set_id", "scoring_config", "job_id", "status"]
    with out_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)
    print(f"\n[done] {len(out_rows)} row(s) → {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
