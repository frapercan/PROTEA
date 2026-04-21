"""Overnight retry for v6 reranker after cross-snapshot fix (2026-04-19).

Differences vs overnight_v6.py:
* No pre-existing smoke dependency — queues all 9 cells fresh at 5000 rounds.
* Runs alongside an external validating smoke (v6-fixsmoke-nk-bpo at 500 rounds,
  job b91c8b06). The smoke runs first because worker-training is serialised;
  if it fails (e.g. KNN bug persists), the following 9 cells will fail fast
  with the same trace, and the orchestrator will abort before evals.

Workflow:
1. Queue 9 training jobs (NK/LK/PK × BPO/MFO/CCO) at 5000 rounds each.
2. Wait for all 9 to reach terminal state.
3. If every training SUCCEEDED, resolve reranker IDs + submit 3 CAFA evals:
   v6 per-aspect nested grid, v4 flat baseline, no-reranker baseline.
4. Wait for evals + log summaries to logs/overnight_v6.log.

Failures do not cascade — any step failure is logged, evals are skipped on
partial success, and the script exits so triage can happen on resume.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import select

from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

API = "http://127.0.0.1:8000"

# Identifiers fixed for this run (ProstT5-XL, GOA v220→v230) — match overnight_v6.py
EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"
OLD_ANN = "6892cb7c-514c-496f-9837-46ba9c1744c4"
NEW_ANN = "73786fd1-ab94-47e1-8f1a-749d86f79780"
ONTO = "6d399baf-339b-4496-9aae-ed75d03229ea"
EVAL_SET = "a73cb77c-9adf-4d55-b61f-c0b1bd05be01"
PRED_SET_BENCH = "4b734d30-29b3-48ce-a1f7-e9cf3b57156d"

V4_MODELS_FLAT = {
    "nk": "c0ab6d4a-8fc9-40d9-a49b-ba069a70ccb7",
    "lk": "b048ac04-653c-4d24-a897-d13c35c536e6",
    "pk": "255836b7-92bd-4513-b6dc-aeb91fd6834a",
}

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


def submit_train(category: str, aspect: str) -> str:
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
            "num_boost_round": 5000,
            "early_stopping_rounds": 50,
        },
        "meta": {"experiment_label": "v6-overnight-retry-post-xsnap-fix"},
    }
    r = requests.post(f"{API}/jobs", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["id"]


def wait_for_jobs(job_ids: list[str], poll_interval: float = 30.0, timeout: float = 20 * 3600) -> dict[str, str]:
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
    log("overnight_v6_retry START (post cross-snapshot fix)")

    submitted: dict[tuple[str, str], str] = {}
    log("queueing 9 v6 training cells (5000 rounds each)...")
    for c in CATEGORIES:
        for a in ASPECTS:
            jid = submit_train(c, a)
            submitted[(c, a)] = jid
            log(f"  queued v6-{c}-{a} → {jid}")

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

    grid = resolve_v6_reranker_ids()
    if grid is None:
        log("ABORT: could not resolve all 9 v6 rerankers in DB")
        return
    log("v6 nested grid resolved:")
    for c, asp_map in grid.items():
        for a, rid in asp_map.items():
            log(f"  {c}×{a} → {rid}")

    log("submitting 3 CAFA evaluations...")
    eval_jobs: dict[str, str] = {}
    eval_jobs["v6_nested"] = submit_eval("v6-nested-eval", rerankers=grid)
    log(f"  v6 nested eval → {eval_jobs['v6_nested']}")
    eval_jobs["v4_flat"] = submit_eval("v4-flat-eval", rerankers=None, flat_ids=V4_MODELS_FLAT)
    log(f"  v4 flat eval → {eval_jobs['v4_flat']}")
    eval_jobs["baseline"] = submit_eval("baseline-no-reranker", rerankers=None)
    log(f"  baseline eval → {eval_jobs['baseline']}")

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

    log("overnight_v6_retry DONE")


if __name__ == "__main__":
    main()
