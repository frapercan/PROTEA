#!/usr/bin/env python
"""Phase A launcher — ablation predictions.

Submits `predict_go_terms` jobs for every (EmbeddingConfig × K) combination in the
8-PLM benchmark ablation. Queries are restricted to the delta proteins (NK+LK) of
the target EvaluationSet, so each run processes ~6k proteins instead of ~500k.

The runner writes a TSV (model, model_name, K, job_id, status, prediction_set_id)
to ``results/ablation_predictions_<timestamp>.tsv`` for the Phase B launcher to
consume.

Usage
-----
    poetry run python scripts/run_ablation_predictions.py \\
        --eval-set       a73cb77c-9adf-4d55-b61f-c0b1bd05be01 \\
        --annotation-set 6892cb7c-514c-496f-9837-46ba9c1744c4 \\
        --snapshot       6d399baf-339b-4496-9aae-ed75d03229ea \\
        [--k 3,5,10] [--only esmc_300m,esm2_650m,...] [--dry-run] \\
        [--api http://localhost:8000]
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


def _delta_accessions(session, eval_set_id: uuid.UUID) -> list[str]:
    e = session.get(EvaluationSet, eval_set_id)
    if e is None:
        raise SystemExit(f"EvaluationSet {eval_set_id} not found")
    data, _ = load_evaluation_data_for_set(session, e)
    return sorted(set(data.nk.keys()) | set(data.lk.keys()) | set(data.pk.keys()))


def _list_embedding_configs(api: str) -> list[dict]:
    r = requests.get(f"{api}/embeddings/configs", timeout=30)
    r.raise_for_status()
    return r.json()


def _short_label(model_name: str) -> str:
    low = model_name.lower()
    if "esm2_t36_3b" in low:
        return "esm2_3b"
    if "esm2_t33_650m" in low:
        return "esm2_650m"
    if "esmc_300m" in low:
        return "esmc_300m"
    if "esmc_600m" in low:
        return "esmc_600m"
    if "ankh-base" in low:
        return "ankh_base"
    if "ankh-large" in low:
        return "ankh_large"
    if "prostt5" in low:
        return "prostt5_xl"
    if "prot_t5_xl" in low:
        return "prott5_xl"
    return low.split("/")[-1]


def submit_predict(
    api: str,
    emb_config_id: str,
    annotation_set_id: str,
    snapshot_id: str,
    limit: int,
    query_accessions: list[str],
    label: str,
) -> str:
    body = {
        "operation": "predict_go_terms",
        "queue_name": "protea.jobs",
        "payload": {
            "embedding_config_id": emb_config_id,
            "annotation_set_id": annotation_set_id,
            "ontology_snapshot_id": snapshot_id,
            "query_accessions": query_accessions,
            "limit_per_entry": limit,
            "compute_alignments": True,
            "compute_taxonomy": True,
            "compute_reranker_features": True,
            "aspect_separated_knn": True,
            "search_backend": "numpy",
        },
        "meta": {"experiment_label": label},
    }
    r = requests.post(f"{api}/jobs", json=body, timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def wait_for(api: str, job_id: str, poll: float = 10.0, timeout: float = 7200.0) -> dict:
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


def find_prediction_set(api: str, job_id: str) -> str | None:
    r = requests.get(f"{api}/jobs/{job_id}/events", timeout=30)
    r.raise_for_status()
    for ev in reversed(r.json()):
        fields = ev.get("fields") or ev.get("payload") or {}
        if "prediction_set_id" in fields:
            return fields["prediction_set_id"]
    r = requests.get(f"{api}/jobs/{job_id}", timeout=30)
    r.raise_for_status()
    result = r.json().get("result") or {}
    return result.get("prediction_set_id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval-set", required=True)
    ap.add_argument("--annotation-set", required=True,
                    help="KNN reference AnnotationSet (e.g. GOA 220).")
    ap.add_argument("--snapshot", required=True,
                    help="OntologySnapshot for prediction (should be the pivot).")
    ap.add_argument("--k", default="3,5,10")
    ap.add_argument("--only", default="",
                    help="Comma-separated short labels to include (default: all).")
    ap.add_argument("--min-embeddings", type=int, default=1,
                    help="Skip configs whose embedding_count is below this (default: 1).")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--output", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-wait", action="store_true",
                    help="Submit and exit — do not poll.")
    ap.add_argument("--poll", type=float, default=15.0)
    args = ap.parse_args()

    # 1. Resolve delta accessions (inline DB read — API endpoints are stale).
    settings = load_settings(Path("."))
    factory = build_session_factory(settings.db_url)
    with factory() as session:
        accessions = _delta_accessions(session, uuid.UUID(args.eval_set))
    print(f"[delta] {len(accessions)} delta proteins (NK ∪ LK)", flush=True)

    # 2. List available embedding configs.
    configs = _list_embedding_configs(args.api)
    only = set(a.strip() for a in args.only.split(",") if a.strip())
    ks = [int(x) for x in args.k.split(",")]

    ready = []
    for c in configs:
        label = _short_label(c["model_name"])
        if only and label not in only:
            continue
        if (c.get("embedding_count") or 0) < args.min_embeddings:
            print(f"[skip] {label:12s} embeddings={c.get('embedding_count')}  <  {args.min_embeddings}")
            continue
        ready.append((label, c))
    print(f"[plan] {len(ready)} model(s) × {len(ks)} K = {len(ready)*len(ks)} job(s)", flush=True)
    if not ready:
        return 1

    if args.dry_run:
        for label, c in ready:
            for k in ks:
                print(f"  [dry] {label}/k={k}  config={c['id']}")
        return 0

    # 3. Submit.
    submitted: list[dict] = []
    for label, c in ready:
        for k in ks:
            spec_label = f"ablation_{label}_k{k}"
            try:
                job_id = submit_predict(
                    args.api,
                    emb_config_id=c["id"],
                    annotation_set_id=args.annotation_set,
                    snapshot_id=args.snapshot,
                    limit=k,
                    query_accessions=accessions,
                    label=spec_label,
                )
                print(f"[submit] {spec_label:30s} job={job_id}", flush=True)
                submitted.append({
                    "model": label,
                    "model_name": c["model_name"],
                    "embedding_config_id": c["id"],
                    "k": k,
                    "job_id": job_id,
                    "label": spec_label,
                })
            except requests.HTTPError as exc:
                print(f"[err] {spec_label}: {exc.response.status_code} {exc.response.text}",
                      flush=True)

    # 4. Wait / resolve.
    rows: list[dict] = []
    if args.no_wait:
        for s in submitted:
            rows.append({**s, "status": "QUEUED", "prediction_set_id": ""})
    else:
        for s in submitted:
            print(f"[poll] {s['label']} ({s['job_id']}) …", end=" ", flush=True)
            try:
                job = wait_for(args.api, s["job_id"], poll=args.poll)
                status = job["status"]
                ps_id = find_prediction_set(args.api, s["job_id"]) if status == "SUCCEEDED" else ""
                print(f"{status}  prediction_set_id={ps_id}", flush=True)
                rows.append({**s, "status": status, "prediction_set_id": ps_id or ""})
            except TimeoutError as exc:
                print(f"TIMEOUT {exc}", flush=True)
                rows.append({**s, "status": "TIMEOUT", "prediction_set_id": ""})

    # 5. Write TSV manifest for Phase B.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(args.output) if args.output else Path(f"results/ablation_predictions_{stamp}.tsv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["model", "model_name", "embedding_config_id", "k", "job_id", "label",
            "status", "prediction_set_id"]
    with out_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"\n[done] {len(rows)} row(s) → {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
