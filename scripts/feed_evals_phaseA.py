#!/usr/bin/env python
"""Background feeder — submits Phase B evals as Phase A coords finish.

Polls the 18 Phase A predict_go_terms coordinator jobs. When one reaches
SUCCEEDED, looks up its prediction_set_id in job_event, and submits one
evaluation job per scoring config (5 by default).

Writes results/ablation_evaluations_phaseB.tsv incrementally.
"""
from __future__ import annotations

import csv
import time
from pathlib import Path

import requests
from sqlalchemy import text

from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

API = "http://localhost:8000"
EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
POLL_SEC = 30


def fetch_scorings() -> dict[str, str]:
    """Pull ``{name: id}`` for every preset scoring config from the API.

    Avoids hardcoding UUIDs: names are stable, UUIDs are regenerated on
    every DB wipe. Script still runs on a fresh DB after re-seeding the
    presets via ``POST /scoring/configs/presets``.
    """
    r = requests.get(f"{API}/scoring/configs", timeout=10)
    r.raise_for_status()
    return {c["name"]: c["id"] for c in r.json()}

MANIFEST = Path("results/ablation_predictions_phaseA.tsv")
OUT = Path("results/ablation_evaluations_phaseB.tsv")
OUT.parent.mkdir(parents=True, exist_ok=True)


def lookup_pred_set_id(sess, job_id: str) -> str | None:
    row = sess.execute(text("""
        select fields->>'prediction_set_id'
        from job_event
        where job_id=:j and fields->>'prediction_set_id' is not null
        limit 1
    """), {"j": job_id}).first()
    return row[0] if row else None


def submit(pred_set_id: str, scoring_id: str) -> str:
    r = requests.post(
        f"{API}/annotations/evaluation-sets/{EVAL_SET}/run",
        json={"prediction_set_id": pred_set_id, "scoring_config_id": scoring_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["id"]


def main() -> int:
    scorings = fetch_scorings()
    print(f"[feed] resolved {len(scorings)} scoring configs from API: "
          f"{', '.join(scorings.keys())}", flush=True)

    with MANIFEST.open() as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    pending: dict[str, dict] = {r["job_id"]: r for r in rows}
    print(f"[feed] watching {len(pending)} Phase A coords", flush=True)

    out_rows: list[dict] = []
    if OUT.exists():
        with OUT.open() as fh:
            out_rows = list(csv.DictReader(fh, delimiter="\t"))
        already = {(r["model"], r["k"], r["scoring_config"]) for r in out_rows}
        print(f"[feed] resuming with {len(out_rows)} already-submitted evals", flush=True)
    else:
        already = set()

    def flush_out() -> None:
        cols = ["model", "k", "prediction_set_id", "scoring_config", "eval_job_id", "status"]
        with OUT.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
            w.writeheader()
            w.writerows(out_rows)

    settings = load_settings(Path("."))
    factory = build_session_factory(settings.db_url)

    while pending:
        done_ids: list[str] = []
        with factory() as sess:
            for job_id, row in list(pending.items()):
                try:
                    j = requests.get(f"{API}/jobs/{job_id}", timeout=20).json()
                except Exception as exc:
                    print(f"[warn] {job_id[:8]} GET failed: {exc}", flush=True)
                    continue
                status = j.get("status")
                if status == "succeeded":
                    pred_set_id = lookup_pred_set_id(sess, job_id)
                    if not pred_set_id:
                        print(f"[warn] {job_id[:8]} succeeded but no pred_set_id yet", flush=True)
                        continue
                    for name, scoring_id in scorings.items():
                        key = (row["model"], row["k"], name)
                        if key in already:
                            continue
                        try:
                            eid = submit(pred_set_id, scoring_id)
                            out_rows.append({
                                "model": row["model"], "k": row["k"],
                                "prediction_set_id": pred_set_id,
                                "scoring_config": name,
                                "eval_job_id": eid,
                                "status": "QUEUED",
                            })
                            already.add(key)
                            print(f"[feed] {row['model']:12s} k={row['k']:>2}  {name:24s}  → {eid[:8]}", flush=True)
                        except requests.HTTPError as exc:
                            print(f"[err] {row['model']}/k{row['k']}/{name}: "
                                  f"{exc.response.status_code} {exc.response.text[:200]}", flush=True)
                    flush_out()
                    done_ids.append(job_id)
                elif status in ("failed", "cancelled"):
                    print(f"[skip] {row['model']:12s} k={row['k']} → {status}", flush=True)
                    done_ids.append(job_id)
        for jid in done_ids:
            pending.pop(jid, None)
        if pending:
            time.sleep(POLL_SEC)

    print(f"[feed] done — {len(out_rows)} eval jobs submitted → {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
