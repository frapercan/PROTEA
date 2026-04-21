"""v8-full: per-(category, aspect) reranker, multisnap GOA deltas, ALL features.

Architecture:
* 9 rerankers (NK/LK/PK × BPO/MFO/CCO) — per-cell training scope, like v6.
* Multi-snapshot training over 12 consecutive GOA deltas (v160→v220), like v7.
* All feature families enabled:
    - compute_alignments=True (BLAST/local alignment score features)
    - compute_taxonomy=True
    - expand_votes_to_ancestors=True
    - use_embedding_pca=True

Hypothesis: combine v6's per-cell specialisation with v7's multisnap data mass
AND bring back the alignment/taxonomy/ancestor features that v6 had but v7
dropped. Targets the 9/9 record set — especially PK cells where v6 still holds.

Training deltas: (160→165), (165→170), ..., (215→220) — 12 pairs total.
Hold-out: v220→v230 (eval_set a73cb77c-*).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API = "http://127.0.0.1:8000"

EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"  # ProstT5-XL
ONTO = "6d399baf-339b-4496-9aae-ed75d03229ea"
EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
PRED_SET_BENCH = "4b734d30-29b3-48ce-a1f7-e9cf3b57156d"

# GOA snapshots v160→v220 stride 5; v211 replaces v210 (no v210 release).
TRAIN_VERSIONS = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 211, 215, 220]
TEST_VERSIONS = [230]

MODEL_NAME = "v8-full"

LOG_FILE = Path(__file__).resolve().parents[1] / "logs" / "overnight_v8.log"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    line = f"[{_now()}] {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def submit_training() -> str:
    payload = {
        "operation": "train_reranker_auto",
        "queue_name": "protea.training",
        "payload": {
            "name": MODEL_NAME,
            "embedding_config_id": EMB_CFG,
            "ontology_snapshot_id": ONTO,
            "train_versions": TRAIN_VERSIONS,
            "test_versions": TEST_VERSIONS,
            "training_scope": "per_cell",
            "per_cell_min_positives": 200,
            "reranker_objective": "lambdarank",
            "limit_per_entry": 5,
            "search_backend": "faiss",
            "compute_alignments": True,
            "compute_taxonomy": True,
            "expand_votes_to_ancestors": True,
            "use_embedding_pca": True,
            "num_boost_round": 5000,
            "early_stopping_rounds": 50,
        },
        "meta": {"experiment_label": "v8-full-training"},
    }
    r = requests.post(f"{API}/jobs", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def wait_for_job(jid: str, poll_interval: float = 60.0, timeout: float = 24 * 3600) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        try:
            j = requests.get(f"{API}/jobs/{jid}", timeout=15).json()
        except Exception as exc:
            log(f"  poll error for {jid}: {exc}")
            continue
        st = j["status"].upper()
        pct = j.get("progress_current") or "?"
        tot = j.get("progress_total") or "?"
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st == "FAILED":
                log(f"  {jid} → FAILED: {j.get('error_message', '')[:400]}")
            else:
                log(f"  {jid} → {st}")
            return st
        log(f"  {jid} still {st} ({pct}/{tot})")
    log(f"  {jid} → TIMEOUT")
    return "TIMEOUT"


def resolve_per_cell_ids() -> dict[str, dict[str, str]] | None:
    """Resolve the 9 per-cell rerankers {cat}-{aspect} produced by v8-full."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from sqlalchemy import select
    from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
    from protea.infrastructure.session import build_session_factory
    from protea.infrastructure.settings import load_settings

    settings = load_settings(Path(__file__).resolve().parents[1])
    factory = build_session_factory(settings.db_url)
    out: dict[str, dict[str, str]] = {"nk": {}, "lk": {}, "pk": {}}
    with factory() as s:
        for cat in ("nk", "lk", "pk"):
            for asp in ("bpo", "mfo", "cco"):
                name = f"{MODEL_NAME}-{cat}-{asp}"
                r = s.execute(
                    select(RerankerModel)
                    .filter(RerankerModel.name == name)
                    .order_by(RerankerModel.created_at.desc())
                ).scalars().first()
                if r is None:
                    log(f"  MISSING reranker: {name}")
                    continue
                out[cat][asp] = str(r.id)
                log(f"  resolved {cat}-{asp} → {r.id}")
    total = sum(len(v) for v in out.values())
    log(f"  resolved {total}/9 per-cell rerankers")
    return out if total > 0 else None


def submit_eval(rerankers: dict[str, dict[str, str]]) -> str:
    body = {
        "operation": "run_cafa_evaluation",
        "queue_name": "protea.jobs",
        "payload": {
            "evaluation_set_id": EVAL_SET,
            "prediction_set_id": PRED_SET_BENCH,
            "rerankers": rerankers,
        },
        "meta": {"experiment_label": "v8-full-eval"},
    }
    r = requests.post(f"{API}/jobs", json=body, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log("=" * 60)
    log(f"overnight_v8 START — name={MODEL_NAME}")
    log(f"  scope=per_cell (9 rerankers), {len(TRAIN_VERSIONS) - 1} delta pairs")
    log(f"  features: alignments+taxonomy+ancestors+pca ALL ON")
    log(f"  train_versions={TRAIN_VERSIONS}")
    log(f"  test_versions={TEST_VERSIONS}")

    jid = submit_training()
    log(f"submitted training job: {jid}")

    status = wait_for_job(jid, poll_interval=120.0, timeout=30 * 3600)
    if status != "SUCCEEDED":
        log(f"ABORT: training ended with status {status}")
        return

    log("resolving per-cell reranker ids...")
    rerankers = resolve_per_cell_ids()
    if rerankers is None:
        log("ABORT: no v8 rerankers resolved in DB")
        return

    log("submitting eval job...")
    eval_jid = submit_eval(rerankers)
    log(f"eval job: {eval_jid}")

    eval_status = wait_for_job(eval_jid, poll_interval=20.0, timeout=6 * 3600)
    log(f"eval job {eval_jid} → {eval_status}")

    if eval_status == "SUCCEEDED":
        try:
            j = requests.get(f"{API}/jobs/{eval_jid}", timeout=15).json()
            result = j.get("result", {}) or {}
            eid = result.get("evaluation_result_id")
            results = result.get("results", {})
            log(f"  evaluation_result_id={eid}")
            fmaxs = []
            for cat in ("NK", "LK", "PK"):
                for asp in ("BPO", "MFO", "CCO"):
                    cell = (results.get(cat) or {}).get(asp) or {}
                    fm = cell.get("fmax")
                    if fm is not None:
                        fmaxs.append(float(fm))
                        log(f"    {cat}-{asp}: {float(fm):.4f}")
            if fmaxs:
                log(f"  avg_fmax={sum(fmaxs) / len(fmaxs):.4f} ({len(fmaxs)} cells)")
            log(f"  raw={json.dumps(results, indent=2)[:1200]}")
        except Exception as exc:
            log(f"  could not fetch results: {exc}")

    log("overnight_v8 DONE")


if __name__ == "__main__":
    main()
