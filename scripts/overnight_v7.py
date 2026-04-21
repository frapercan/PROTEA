"""v7-aa-multisnap: one reranker per category, aspect as categorical feature,
trained across 12 consecutive GOA deltas (v160→v220 in stride 5).

Architecture:
* 3 rerankers (NK / LK / PK) — no per-aspect split.
* aspect ∈ {P, F, C} fed as a LightGBM categorical feature so the tree can
  bifurcate by aspect when the signal warrants, while NK keeps the full
  cross-aspect data mass (where v6 per-cell starved itself).

Training deltas: (160→165), (165→170), ..., (215→220) — 12 pairs total.
Hold-out: v220→v230 (the existing eval_set a73cb77c).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API = "http://127.0.0.1:8000"

EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"
ONTO = "6d399baf-339b-4496-9aae-ed75d03229ea"
EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
PRED_SET_BENCH = "4b734d30-29b3-48ce-a1f7-e9cf3b57156d"

# GOA snapshots from v160 to v220 in stride 5; v211 breaks the stride (no v210
# release). Treat 211 as the 210 step so the delta chain stays consecutive.
TRAIN_VERSIONS = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 211, 215, 220]
TEST_VERSIONS = [230]

MODEL_NAME = "v7-aa-multisnap"

LOG_FILE = Path(__file__).resolve().parents[1] / "logs" / "overnight_v7.log"


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
            "training_scope": "per_category",
            "reranker_objective": "lambdarank",
            "limit_per_entry": 5,
            "search_backend": "faiss",
            "compute_alignments": False,
            "compute_taxonomy": True,
            "expand_votes_to_ancestors": False,
            "use_embedding_pca": True,
            "num_boost_round": 5000,
            "early_stopping_rounds": 50,
        },
        "meta": {"experiment_label": "v7-aa-multisnap-training"},
    }
    r = requests.post(f"{API}/jobs", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def wait_for_job(jid: str, poll_interval: float = 60.0, timeout: float = 20 * 3600) -> str:
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


def resolve_reranker_ids() -> dict[str, str] | None:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from sqlalchemy import select
    from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
    from protea.infrastructure.session import build_session_factory
    from protea.infrastructure.settings import load_settings

    settings = load_settings(Path(__file__).resolve().parents[1])
    factory = build_session_factory(settings.db_url)
    out: dict[str, str] = {}
    with factory() as s:
        for cat in ("nk", "lk", "pk"):
            name = f"{MODEL_NAME}-{cat}"
            r = s.execute(
                select(RerankerModel)
                .filter(RerankerModel.name == name)
                .order_by(RerankerModel.created_at.desc())
            ).scalars().first()
            if r is None:
                log(f"  missing reranker: {name}")
                return None
            out[cat] = str(r.id)
            log(f"  resolved {cat} → {r.id}")
    return out


def submit_eval(flat_ids: dict[str, str]) -> str:
    body = {
        "operation": "run_cafa_evaluation",
        "queue_name": "protea.jobs",
        "payload": {
            "evaluation_set_id": EVAL_SET,
            "prediction_set_id": PRED_SET_BENCH,
            "reranker_id_nk": flat_ids["nk"],
            "reranker_id_lk": flat_ids["lk"],
            "reranker_id_pk": flat_ids["pk"],
        },
        "meta": {"experiment_label": "v7-aa-multisnap-eval"},
    }
    r = requests.post(f"{API}/jobs", json=body, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log("=" * 60)
    log(f"overnight_v7 START — name={MODEL_NAME}, {len(TRAIN_VERSIONS) - 1} delta pairs")
    log(f"  train_versions={TRAIN_VERSIONS}")
    log(f"  test_versions={TEST_VERSIONS}")

    jid = submit_training()
    log(f"submitted training job: {jid}")

    status = wait_for_job(jid)
    if status != "SUCCEEDED":
        log(f"ABORT: training ended with status {status}")
        return

    log("resolving reranker ids...")
    flat_ids = resolve_reranker_ids()
    if flat_ids is None:
        log("ABORT: could not resolve all 3 v7 rerankers in DB")
        return

    log("submitting eval job...")
    eval_jid = submit_eval(flat_ids)
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
            log(f"  results={json.dumps(results, indent=2)[:1200]}")
        except Exception as exc:
            log(f"  could not fetch results: {exc}")

    log("overnight_v7 DONE")


if __name__ == "__main__":
    main()
