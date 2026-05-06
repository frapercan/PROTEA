"""LAFA-compatible PROTEA wrapper (frozen-data + reranker variant).

Entry point that honours the LAFA container CLI plus a
``--frozen_data_dir`` flag pointing at a bind-mounted bundle. The
bundle must contain the artifacts produced by the PROTEA exporter
``scripts/export_lafa_bundle.py`` (F-LAFA.1, separate slice):

::

    <bundle>/
    ├── manifest.json                # cutoff version + schema sha
    ├── reference_embeddings.parquet # (accession, embedding) for refs
    ├── reference_annotations.parquet# (accession, go_term_id, ...) per ref
    ├── go_term_metadata.parquet     # (go_term_id, go_id, aspect, name, ia_weight)
    ├── pca_state.npz                # mean (D,) + components (16, D) for query PCA
    ├── anc2vec.npz                  # Anc2Vec GO-term embedding dictionary
    └── reranker.txt                 # LightGBM booster (text format)

Pipeline:
    1. Embed query FASTA via ``prott5_encoder`` (mean-pool ProtT5).
    2. Load the bundle into memory (parquet / npz / text).
    3. Call ``protea_method.pipeline.predict`` with the frozen
       references, reranker booster, and v6 feature inputs.
    4. Write predictions: ``<query>\\t<go_id>\\t<reranker_score>`` per
       (query, go_term) candidate. Fallback to ``1 - distance`` when
       no booster ships in the bundle.

Strict design rule: this module owns no compute logic. KNN, feature
enrichment, reranker scoring all live in ``protea_method``. The
container's job is FASTA in, frozen-data load, library call, TSV out.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np

# Make ``protea_method`` importable when the container is run from a
# checkout (the published wheel is the canonical install path; this
# fallback is only for local development).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pyarrow.parquet as pq  # noqa: E402
from protea_method.anc2vec import Anc2VecIndex  # noqa: E402
from protea_method.pipeline import PredictConfig, predict  # noqa: E402
from protea_method.reranker import load_from_bytes  # noqa: E402
from prott5_encoder import embed_sequences, parse_fasta  # noqa: E402


def _open_text(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def _open_output(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", newline="")
    return open(path, "w", newline="")


def _load_manifest(bundle: Path) -> dict[str, Any]:
    return json.loads((bundle / "manifest.json").read_text())


def _load_refs(bundle: Path) -> tuple[list[str], np.ndarray]:
    table = pq.read_table(bundle / "reference_embeddings.parquet")
    accs = [str(a) for a in table.column("accession").to_pylist()]
    raw = table.column("embedding").to_pylist()
    embeddings = np.asarray(raw, dtype=np.float32)
    if embeddings.ndim == 1:
        embeddings = np.stack([np.asarray(v, dtype=np.float32) for v in raw])
    return accs, embeddings


def _load_annotations(bundle: Path) -> dict[str, list[dict[str, Any]]]:
    table = pq.read_table(bundle / "reference_annotations.parquet")
    rows = table.to_pylist()
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row["accession"]), []).append(
            {k: v for k, v in row.items() if k != "accession"},
        )
    return out


def _load_go_metadata(bundle: Path) -> tuple[dict[int, str], dict[int, str]]:
    table = pq.read_table(bundle / "go_term_metadata.parquet")
    rows = table.to_pylist()
    go_id_map: dict[int, str] = {}
    go_aspect_map: dict[int, str] = {}
    for row in rows:
        gid = int(row["go_term_id"])
        go_id_map[gid] = str(row["go_id"])
        go_aspect_map[gid] = str(row.get("aspect") or "")
    return go_id_map, go_aspect_map


def _load_pca_state(bundle: Path) -> tuple[np.ndarray, np.ndarray] | None:
    pca_path = bundle / "pca_state.npz"
    if not pca_path.exists():
        return None
    data = np.load(pca_path)
    return (
        np.asarray(data["mean"], dtype=np.float32),
        np.asarray(data["components"], dtype=np.float32),
    )


def _load_booster_bytes(bundle: Path) -> bytes | None:
    booster_path = bundle / "reranker.txt"
    if not booster_path.exists():
        return None
    return booster_path.read_bytes()


def _stack_query_embeddings(
    embeddings: dict[str, np.ndarray],
    order: list[str],
) -> tuple[np.ndarray, list[str]]:
    """Stack in input FASTA order, dropping accessions that failed to embed."""
    kept: list[str] = []
    rows: list[np.ndarray] = []
    for acc in order:
        vec = embeddings.get(acc)
        if vec is None:
            continue
        kept.append(acc)
        rows.append(vec)
    if not rows:
        return np.empty((0, 0), dtype=np.float32), kept
    return np.stack(rows).astype(np.float32, copy=False), kept


def _score_for_output(pred: dict[str, Any]) -> float:
    """Pick the reranker score when present, otherwise fall back to ``1 - distance``."""
    if "reranker_score" in pred:
        return float(pred["reranker_score"])
    distance = float(pred.get("min_distance", pred.get("distance", 1.0)))
    return max(0.0, 1.0 - distance)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "LAFA-compatible PROTEA wrapper. Embeds queries with ProtT5, then "
            "runs KNN + v6 features + LightGBM reranker against a frozen "
            "reference bundle."
        ),
    )
    parser.add_argument("--query_file", "-q", required=True, help="FASTA of query sequences.")
    parser.add_argument(
        "--frozen_data_dir",
        required=True,
        help="Bind-mounted bundle directory (see module docstring for layout).",
    )
    parser.add_argument(
        "--output_baseline",
        "-o",
        required=True,
        help="Output TSV: <query>\\t<go_id>\\t<score>. Gzipped if suffix is .gz.",
    )
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--metric", default="cosine", choices=["cosine", "l2"])
    parser.add_argument("--backend", default="numpy", choices=["numpy", "faiss"])
    parser.add_argument(
        "--aspect_separated",
        action="store_true",
        help="Run one KNN per GO aspect (P / F / C). Improves BPO recall.",
    )
    parser.add_argument(
        "--no_v6",
        action="store_true",
        help="Skip the v6 feature enrichment pass (faster, less accurate).",
    )
    parser.add_argument(
        "--no_reranker",
        action="store_true",
        help="Skip the LightGBM reranker even if reranker.txt is in the bundle.",
    )
    parser.add_argument(
        "--model_dir",
        default=os.environ.get("HF_CACHE"),
        help="HuggingFace cache dir for ProtT5 (default: $HF_CACHE).",
    )
    args = parser.parse_args()

    bundle = Path(args.frozen_data_dir)
    if not bundle.is_dir():
        print(f"[protea_main] frozen_data_dir not a directory: {bundle}", file=sys.stderr)
        sys.exit(1)

    manifest = _load_manifest(bundle)
    print(
        f"[protea_main] bundle: cutoff={manifest.get('cutoff_version', '?')} "
        f"schema_sha={manifest.get('feature_schema_sha', '?')[:12]}",
    )

    print(f"[protea_main] reading queries: {args.query_file}")
    query_seqs = parse_fasta(args.query_file)
    if not query_seqs:
        print("[protea_main] no query sequences parsed; aborting.", file=sys.stderr)
        sys.exit(2)
    query_order = list(query_seqs.keys())
    print(f"[protea_main] {len(query_seqs)} query sequences")

    print("[protea_main] embedding queries with ProtT5 (mean-pool)")
    query_embeddings_by_acc = embed_sequences(query_seqs, cache_dir=args.model_dir)
    Q, kept_q = _stack_query_embeddings(query_embeddings_by_acc, query_order)
    if Q.size == 0:
        print("[protea_main] all queries failed to embed; aborting.", file=sys.stderr)
        sys.exit(3)

    print("[protea_main] loading frozen bundle")
    ref_accs, R = _load_refs(bundle)
    annotations = _load_annotations(bundle)
    go_id_map, go_aspect_map = _load_go_metadata(bundle)
    pca_state = _load_pca_state(bundle) if not args.no_v6 else None
    anc_idx = (
        Anc2VecIndex(bundle / "anc2vec.npz")
        if (bundle / "anc2vec.npz").exists() and not args.no_v6
        else None
    )
    booster_bytes = _load_booster_bytes(bundle) if not args.no_reranker else None
    booster = load_from_bytes(booster_bytes) if booster_bytes is not None else None
    print(
        f"[protea_main] refs={len(ref_accs)} annotations={sum(len(v) for v in annotations.values())} "
        f"go_terms={len(go_id_map)} pca={pca_state is not None} "
        f"anc2vec={anc_idx is not None} reranker={booster is not None}",
    )

    config = PredictConfig(
        k=args.k,
        metric=args.metric,
        backend=args.backend,
        aspect_separated=args.aspect_separated,
        compute_v6_features=not args.no_v6 and anc_idx is not None,
        compute_taxonomy=False,  # taxonomy pair features not bundled in the canonical export
        pre_normalized=False,
    )
    print(f"[protea_main] running predict (config={config})")
    predictions = predict(
        query_accessions=kept_q,
        query_embeddings=Q,
        reference_accessions=ref_accs,
        reference_embeddings=R,
        annotations=annotations,
        go_id_map=go_id_map,
        go_aspect_map=go_aspect_map,
        config=config,
        pca_state=pca_state,
        booster=booster,
        anc_idx=anc_idx,
    )
    print(f"[protea_main] {len(predictions)} prediction rows")

    out_path = args.output_baseline
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    n_rows = 0
    with _open_output(out_path) as fh:
        writer = csv.writer(fh, delimiter="\t")
        for pred in predictions:
            go_id = go_id_map.get(int(pred["go_term_id"]))
            if not go_id:
                continue
            score = _score_for_output(pred)
            writer.writerow([pred["protein_accession"], go_id, f"{score:.4f}"])
            n_rows += 1

    print(f"[protea_main] wrote {n_rows} predictions to {out_path}")


if __name__ == "__main__":
    main()
