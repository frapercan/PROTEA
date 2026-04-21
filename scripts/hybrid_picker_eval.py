"""Hybrid per-cell picker: submits a CAFA evaluation using the best reranker
found on eval_set a73cb77c-* for each (category, aspect) cell.

Data (queried 2026-04-19 from evaluation_result on eval_set a73cb77c):

  cell        fmax    reranker_uuid                          source eval
  NK-BPO      0.3916  372ed701-40bd-4ff1-b910-b1d8f7e01934   5f00b9f7
  NK-MFO      0.5614  347c0ced-3d24-4c65-8f9f-f487a630104d   881bf901 (v7-nk)
  NK-CCO      0.6536  468a547e-d921-4193-b4d2-63b41fd6c905   d995b5e2 (v5-v2-nk)
  LK-BPO      0.4619  07329dab-4c37-4e49-8d74-055cc2eed097   d995b5e2
  LK-MFO      0.5311  8a588663-00aa-470d-8f06-c36eb747fe74   881bf901 (v7-lk)
  LK-CCO      0.6831  07329dab-4c37-4e49-8d74-055cc2eed097   006ebd52  (winner b6c6756a was deleted, fallback to #2)
  PK-BPO      0.1451  3b5408fc-50d3-4812-9709-8be572aa1f53   d1f9999b (v6-pk-bpo)
  PK-MFO      0.2217  cf8ba8fe-57c6-41c7-ab88-a32680b72229   d1f9999b (v6-pk-mfo)
  PK-CCO      0.3121  e8a3067a-9d04-49dd-b44e-bacbba6aa91f   d1f9999b (v6-pk-cco)

Two variants submitted:
  A) no scoring_config       — mirrors d1f9999b (PK records held here)
  B) scoring_config=embedding_only — mirrors the BPO/CCO records

Target: avg_fmax ≥ 0.4407 (strict per-cell ceiling)  vs v6 run 0.4355.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API = "http://127.0.0.1:8000"

EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
PRED_SET = "4b734d30-29b3-48ce-a1f7-e9cf3b57156d"
SCORING_CONFIG_EMB_ONLY = "5fb4c0ed-1aae-438e-b262-200ccee700d8"

RERANKERS: dict[str, dict[str, str]] = {
    "nk": {
        "bpo": "372ed701-40bd-4ff1-b910-b1d8f7e01934",
        "mfo": "347c0ced-3d24-4c65-8f9f-f487a630104d",
        "cco": "468a547e-d921-4193-b4d2-63b41fd6c905",
    },
    "lk": {
        "bpo": "07329dab-4c37-4e49-8d74-055cc2eed097",
        "mfo": "8a588663-00aa-470d-8f06-c36eb747fe74",
        "cco": "07329dab-4c37-4e49-8d74-055cc2eed097",
    },
    "pk": {
        "bpo": "3b5408fc-50d3-4812-9709-8be572aa1f53",
        "mfo": "cf8ba8fe-57c6-41c7-ab88-a32680b72229",
        "cco": "e8a3067a-9d04-49dd-b44e-bacbba6aa91f",
    },
}

LOG_FILE = Path(__file__).resolve().parents[1] / "logs" / "hybrid_picker.log"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    line = f"[{_now()}] {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def submit(variant: str, scoring_id: str | None) -> str:
    payload: dict = {
        "evaluation_set_id": EVAL_SET,
        "prediction_set_id": PRED_SET,
        "rerankers": RERANKERS,
    }
    if scoring_id:
        payload["scoring_config_id"] = scoring_id
    body = {
        "operation": "run_cafa_evaluation",
        "queue_name": "protea.jobs",
        "payload": payload,
        "meta": {"experiment_label": f"hybrid-picker-{variant}"},
    }
    r = requests.post(f"{API}/jobs", json=body, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def wait_for_job(jid: str, poll: float = 20.0, timeout: float = 3 * 3600) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(poll)
        try:
            j = requests.get(f"{API}/jobs/{jid}", timeout=15).json()
        except Exception as exc:
            log(f"  poll error {jid}: {exc}")
            continue
        st = j["status"].upper()
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            log(f"  {jid} → {st}")
            return j
        log(f"  {jid} still {st} ({j.get('progress_current') or '?'}/{j.get('progress_total') or '?'})")
    log(f"  {jid} → TIMEOUT")
    return {}


def summarise(job: dict) -> None:
    result = job.get("result") or {}
    eid = result.get("evaluation_result_id")
    results = result.get("results") or {}
    log(f"  eval_id={eid}")
    fmaxs = []
    for cat in ("NK", "LK", "PK"):
        for asp in ("BPO", "MFO", "CCO"):
            cell = (results.get(cat) or {}).get(asp) or {}
            fm = cell.get("fmax")
            if fm is not None:
                fmaxs.append(float(fm))
                log(f"    {cat}-{asp}: {float(fm):.4f}")
    if fmaxs:
        log(f"  avg_fmax={sum(fmaxs) / len(fmaxs):.4f}  ({len(fmaxs)} cells)")


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log("=" * 60)
    log("hybrid_picker_eval START")

    log("variant A: no scoring_config")
    jid_a = submit("no-scoring", None)
    log(f"  job: {jid_a}")

    log("variant B: scoring_config=embedding_only")
    jid_b = submit("embedding-scoring", SCORING_CONFIG_EMB_ONLY)
    log(f"  job: {jid_b}")

    log("waiting for A...")
    job_a = wait_for_job(jid_a)
    log("== variant A result ==")
    summarise(job_a)

    log("waiting for B...")
    job_b = wait_for_job(jid_b)
    log("== variant B result ==")
    summarise(job_b)

    log("DONE")


if __name__ == "__main__":
    main()
