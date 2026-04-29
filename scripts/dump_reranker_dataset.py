"""Legacy CLI for publishing a frozen re-ranker dataset.

Thin wrapper over ``POST /datasets`` (which enqueues the
``export_research_dataset`` operation on the ``protea.training`` worker
and inserts a ``Dataset`` row once the artifacts land in the configured
artifact store). Polls until the job completes, then fetches the
registered dataset and prints its URIs.

Usage:

    python scripts/dump_reranker_dataset.py \\
        --name bench-v1-K5 \\
        --train-versions 160 165 170 175 180 185 190 195 200 205 211 215 220 \\
        --test-versions 230 \\
        --k 5 \\
        [--embedding <uuid>] [--ontology <uuid>] \\
        [--all-features]
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import requests

API = "http://127.0.0.1:8000"
DEFAULT_EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"  # ProstT5-XL
DEFAULT_ONTO = "35c3ad67-3002-47db-8f71-eeed69d22ad6"  # GO 2026-01-23 (post-wipe pivot)


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True, help="output_name for the Dataset row")
    p.add_argument("--train-versions", nargs="+", type=int, required=True)
    p.add_argument("--test-versions", nargs="+", type=int, required=True)
    p.add_argument("--k", type=int, default=5)
    p.add_argument("--embedding", default=DEFAULT_EMB_CFG)
    p.add_argument("--ontology", default=DEFAULT_ONTO)
    p.add_argument("--search-backend", default="faiss")
    p.add_argument("--annotation-source", default="goa")
    p.add_argument("--all-features", action="store_true",
                   help="enable alignments, taxonomy, ancestor expansion, PCA")
    p.add_argument("--api", default=API)
    p.add_argument("--poll-interval", type=float, default=10.0)
    return p.parse_args()


def main() -> None:
    a = _args()

    body = {
        "output_name": a.name,
        "embedding_config_id": a.embedding,
        "ontology_snapshot_id": a.ontology,
        "train_versions": a.train_versions,
        "test_versions": a.test_versions,
        "annotation_source": a.annotation_source,
        "k": a.k,
        "search_backend": a.search_backend,
        "compute_alignments": bool(a.all_features),
        "compute_taxonomy": bool(a.all_features),
        "expand_votes_to_ancestors": bool(a.all_features),
        "use_embedding_pca": bool(a.all_features),
    }

    r = requests.post(f"{a.api}/datasets", json=body, timeout=30)
    if r.status_code == 409:
        sys.exit(f"dataset name {a.name!r} already exists")
    r.raise_for_status()
    job_id = r.json()["job_id"]
    print(f"[dump] submitted job {job_id}  → dataset {a.name!r}")

    while True:
        time.sleep(a.poll_interval)
        # 60s poll timeout absorbs transient queue pauses under GPU load.
        st = requests.get(f"{a.api}/jobs/{job_id}", timeout=60).json()
        status = st.get("status")
        print(f"[dump] status={status}")
        if status in {"succeeded", "failed", "cancelled"}:
            if status != "succeeded":
                print(json.dumps(st.get("result"), indent=2, default=str))
                sys.exit(f"dump job {status}")
            break

    ds = requests.get(f"{a.api}/datasets/{a.name}", timeout=30).json()
    print("[dump] registered dataset:")
    print(json.dumps(ds, indent=2, default=str))


if __name__ == "__main__":
    main()
