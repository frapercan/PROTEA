"""Overnight orchestration for v6 reranker benchmark.

Workflow:

1. Skip the smoke cell already submitted (NK × BPO, job d7da3d88).
2. Queue the remaining 8 v6 training cells (NK×MFO, NK×CCO, LK×{BPO,MFO,CCO},
   PK×{BPO,MFO,CCO}). They run serially behind the smoke on worker-training.
3. Wait for all 9 trainings to reach a terminal state.
4. If every training succeeded, resolve the reranker IDs by name, then submit
   three CAFA evaluation jobs:
     - v6 per-aspect nested grid
     - v4 flat baseline (already trained)
     - no-reranker baseline (distance-based)
5. Log every step to ``logs/overnight_v6.log`` for morning review.

No arguments. Failures do not cascade — any step failure is logged and the
watchdog aborts the eval phase so the user can triage in the morning.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import select

from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

API = "http://127.0.0.1:8000"

# ── Identifiers fixed for this run (ProstT5-XL, GOA v220→v230) ────────
EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"
OLD_ANN = "6892cb7c-514c-496f-9837-46ba9c1744c4"
NEW_ANN = "73786fd1-ab94-47e1-8f1a-749d86f79780"
ONTO = "6d399baf-339b-4496-9aae-ed75d03229ea"
EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
PRED_SET_BENCH = "4b734d30-29b3-48ce-a1f7-e9cf3b57156d"  # ProstT5-XL K=5 (29200 proteins)
SMOKE_JOB = "d7da3d88-d6da-4aa2-bf55-1c7dd9ef2b27"  # NK × BPO (num_boost_round=500)

V4_MODELS_FLAT = {
    "nk": "c0ab6d4a-8fc9-40d9-a49b-ba069a70ccb7",
    "lk": "b048ac04-653c-4d24-a897-d13c35c536e6",
    "pk": "255836b7-92bd-4513-b6dc-aeb91fd6834a",
}

# 9-cell grid
CATEGORIES = ("nk", "lk", "pk")
ASPECTS = ("bpo", "mfo", "cco")

LOG_FILE = Path(__file__).resolve().parents[1] / "logs" / "overnight_v6.log"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    line = f"[{_now()}] {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def v6_model_name(category: str, aspect: str) -> str:
    return f"v6-prostt5xl-5000r-{category}-{aspect}"


def submit_train(category: str, aspect: str, *, smoke: bool = False) -> str:
    payload = {
        "operation": "train_reranker",
        "queue_name": "protea.training",
        "payload": {
            "name": v6_model_name(category, aspect),
            "old_annotation_set_id": OLD_ANN,
            "new_annotation_set_id": NEW_ANN,
            "embedding_config_id": EMB_CFG,
            "ontology_snapshot_id": ONTO,
            "category": category,
            "aspect": aspect,
            "limit_per_entry": 5,
            "search_backend": "faiss",
            "compute_alignments": False,
            "compute_taxonomy": True,
            "expand_votes_to_ancestors": False,
            "use_embedding_pca": True,
            "num_boost_round": 500 if smoke else 5000,
            "early_stopping_rounds": 30 if smoke else 50,
        },
        "meta": {"experiment_label": "v6-overnight"},
    }
    r = requests.post(f"{API}/jobs", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def wait_for_jobs(job_ids: list[str], poll_interval: float = 30.0, timeout: float = 18 * 3600) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    remaining = set(job_ids)
    final: dict[str, str] = {}
    while remaining and time.monotonic() < deadline:
        time.sleep(poll_interval)
        for jid in list(remaining):
            try:
                j = requests.get(f"{API}/jobs/{jid}", timeout=15).json()
            except Exception as exc:
                log(f"  poll error for {jid}: {exc}")
                continue
            st = j["status"].upper()
            if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
                final[jid] = st
                remaining.discard(jid)
                if st == "FAILED":
                    log(f"  {jid} → FAILED: {j.get('error_message', '')[:300]}")
                else:
                    log(f"  {jid} → {st}")
    if remaining:
        for jid in remaining:
            final[jid] = "TIMEOUT"
            log(f"  {jid} → TIMEOUT")
    return final


def resolve_v6_reranker_ids() -> dict[str, dict[str, str]] | None:
    """Return nested {category: {aspect: reranker_id}} or None if any missing."""
    settings = load_settings(Path(__file__).resolve().parents[1])
    factory = build_session_factory(settings.db_url)
    grid: dict[str, dict[str, str]] = {c: {} for c in CATEGORIES}
    with factory() as s:
        for c in CATEGORIES:
            for a in ASPECTS:
                name = v6_model_name(c, a)
                r = s.execute(
                    select(RerankerModel)
                    .filter(RerankerModel.name == name)
                    .order_by(RerankerModel.created_at.desc())
                ).scalars().first()
                if r is None:
                    log(f"  missing reranker: {name}")
                    return None
                grid[c][a] = str(r.id)
    return grid


def submit_eval(label: str, rerankers: dict | None, flat_ids: dict | None = None) -> str:
    payload_inner = {
        "evaluation_set_id": EVAL_SET,
        "prediction_set_id": PRED_SET_BENCH,
    }
    if rerankers is not None:
        payload_inner["rerankers"] = rerankers
    if flat_ids is not None:
        payload_inner["reranker_id_nk"] = flat_ids["nk"]
        payload_inner["reranker_id_lk"] = flat_ids["lk"]
        payload_inner["reranker_id_pk"] = flat_ids["pk"]

    body = {
        "operation": "run_cafa_evaluation",
        "queue_name": "protea.jobs",
        "payload": payload_inner,
        "meta": {"experiment_label": label},
    }
    r = requests.post(f"{API}/jobs", json=body, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log("=" * 60)
    log("overnight_v6 START")
    log(f"smoke job (already submitted): {SMOKE_JOB}")

    # 1. Queue remaining 8 cells (skip NK × BPO which is the smoke)
    submitted: dict[tuple[str, str], str] = {("nk", "bpo"): SMOKE_JOB}
    log("queueing remaining 8 v6 training cells (5000 rounds each)...")
    for c in CATEGORIES:
        for a in ASPECTS:
            if (c, a) == ("nk", "bpo"):
                continue
            jid = submit_train(c, a, smoke=False)
            submitted[(c, a)] = jid
            log(f"  queued v6-{c}-{a} → {jid}")

    # 2. Wait for all 9
    log(f"waiting for {len(submitted)} training jobs to finish...")
    finals = wait_for_jobs(list(submitted.values()))

    succeeded = [k for k, v in finals.items() if v == "SUCCEEDED"]
    failed = [k for k, v in finals.items() if v != "SUCCEEDED"]
    log(f"training phase done: {len(succeeded)} succeeded, {len(failed)} failed/timeout")
    for (c, a), jid in submitted.items():
        log(f"  [{c}×{a}]  {jid}  {finals.get(jid, '?')}")

    if failed:
        log("ABORT: not submitting evals because some trainings did not succeed")
        return

    # 3. Resolve reranker IDs
    grid = resolve_v6_reranker_ids()
    if grid is None:
        log("ABORT: could not resolve all 9 v6 rerankers in DB")
        return
    log("v6 nested grid resolved:")
    for c, asp_map in grid.items():
        for a, rid in asp_map.items():
            log(f"  {c}×{a} → {rid}")

    # 4. Submit 3 eval jobs
    log("submitting 3 CAFA evaluations...")
    eval_jobs: dict[str, str] = {}
    eval_jobs["v6_nested"] = submit_eval("v6-nested-eval", rerankers=grid)
    log(f"  v6 nested eval → {eval_jobs['v6_nested']}")
    eval_jobs["v4_flat"] = submit_eval("v4-flat-eval", rerankers=None, flat_ids=V4_MODELS_FLAT)
    log(f"  v4 flat eval → {eval_jobs['v4_flat']}")
    eval_jobs["baseline"] = submit_eval("baseline-no-reranker", rerankers=None)
    log(f"  baseline eval → {eval_jobs['baseline']}")

    # 5. Wait for evals
    log("waiting for 3 evaluation jobs to finish...")
    eval_finals = wait_for_jobs(list(eval_jobs.values()), poll_interval=20.0, timeout=6 * 3600)

    log("evaluation phase summary:")
    for label, jid in eval_jobs.items():
        st = eval_finals.get(jid, "?")
        log(f"  [{label}] {jid} → {st}")
        if st == "SUCCEEDED":
            try:
                j = requests.get(f"{API}/jobs/{jid}", timeout=15).json()
                result = j.get("result", {}) or {}
                eid = result.get("evaluation_result_id")
                results = result.get("results", {})
                log(f"    evaluation_result_id={eid}")
                log(f"    results={json.dumps(results, indent=2)[:800]}")
            except Exception as exc:
                log(f"    could not fetch results: {exc}")

    log("overnight_v6 DONE")


if __name__ == "__main__":
    main()
