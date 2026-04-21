"""Launch train_reranker_auto in dump-only mode to export a frozen dataset
for protea-reranker-lab.

Runs KNN + feature generation over the given temporal pairs, concatenates
everything into ``{out}/train.parquet`` + ``{out}/eval.parquet`` + manifest,
and exits WITHOUT training any LightGBM models.

Usage:

    python scripts/dump_reranker_dataset.py \\
        --name bench-v1-K5 \\
        --out /path/to/protea-reranker-lab/datasets/bench-v1-K5 \\
        --train-versions 160 165 170 175 180 185 190 195 200 205 211 215 220 \\
        --test-versions 230 \\
        --k 5 \\
        [--embedding <uuid>] [--ontology <uuid>] \\
        [--all-features]
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

API = "http://127.0.0.1:8000"
DEFAULT_EMB_CFG = "c0ae5b69-d6dc-41cf-a711-1739d3d2e170"  # ProstT5-XL
DEFAULT_ONTO = "6d399baf-339b-4496-9aae-ed75d03229ea"


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--out", required=True, help="absolute path to dataset dir")
    p.add_argument("--train-versions", nargs="+", type=int, required=True)
    p.add_argument("--test-versions", nargs="+", type=int, required=True)
    p.add_argument("--k", type=int, default=5)
    p.add_argument("--embedding", default=DEFAULT_EMB_CFG)
    p.add_argument("--ontology", default=DEFAULT_ONTO)
    p.add_argument("--search-backend", default="faiss")
    p.add_argument("--annotation-source", default="goa")
    p.add_argument("--all-features", action="store_true",
                   help="enable alignments, taxonomy, ancestor expansion, PCA")
    p.add_argument("--queue", default="protea.training")
    p.add_argument("--api", default=API)
    return p.parse_args()


def main() -> None:
    a = _args()
    out_dir = Path(a.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "operation": "train_reranker_auto",
        "queue_name": a.queue,
        "payload": {
            "name": a.name,
            "embedding_config_id": a.embedding,
            "ontology_snapshot_id": a.ontology,
            "train_versions": a.train_versions,
            "test_versions": a.test_versions,
            "annotation_source": a.annotation_source,
            "limit_per_entry": a.k,
            "search_backend": a.search_backend,
            "compute_alignments": bool(a.all_features),
            "compute_taxonomy": bool(a.all_features),
            "expand_votes_to_ancestors": bool(a.all_features),
            "use_embedding_pca": bool(a.all_features),
            "training_scope": "per_cell",
            "dump_to": str(out_dir),
            "dump_only": True,
        },
        "meta": {"experiment_label": f"dump:{a.name}"},
    }

    r = requests.post(f"{a.api}/jobs", json=payload, timeout=15)
    r.raise_for_status()
    job_id = r.json()["id"]
    print(f"[dump] submitted job {job_id}  → {out_dir}")

    while True:
        time.sleep(10)
        st = requests.get(f"{a.api}/jobs/{job_id}", timeout=10).json()
        status = st.get("status")
        print(f"[dump] status={status}")
        if status in {"succeeded", "failed", "cancelled"}:
            print(json.dumps(st.get("result"), indent=2, default=str))
            if status != "succeeded":
                raise SystemExit(f"dump job {status}")
            break

    manifest = out_dir / "manifest.json"
    if manifest.exists():
        print(f"[dump] manifest:\n{manifest.read_text()}")


if __name__ == "__main__":
    main()
