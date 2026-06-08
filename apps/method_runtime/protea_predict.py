"""Thin CLI wrapper for the protea-method-runtime container (ADR-D15).

FASTA in, TSV out. The bundle layout matches
``apps/lafa_container/protea_main.py`` (single source of truth for the
frozen-bundle schema until F-LAFA v2 rebases the LAFA containers on
top of this image).

Pipeline:

1. Embed query FASTA via ``prott5_encoder`` (mean-pool ProtT5).
2. Load the bundle into memory (parquet + npz + boosters).
3. Call ``protea_method.pipeline.predict`` for KNN + v6 features +
   LightGBM rerank.
4. Emit ``<query_accession>\\t<go_id>\\t<score>`` rows, one per
   (query, go_term) candidate. Output path is gzipped when the
   suffix is ``.gz``.

This module owns argparse, bundle loading, and TSV emission. KNN /
feature compute / rerank live in ``protea_method``; the query encoder
lives in ``prott5_encoder``.
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
import pyarrow.parquet as pq


def _open_output(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", newline="")
    return open(path, "w", newline="")


def _load_manifest(bundle: Path) -> dict[str, Any]:
    return json.loads((bundle / "manifest.json").read_text())


def _load_refs(bundle: Path) -> tuple[list[str], np.ndarray]:
    """Load reference (accessions, embeddings) as a contiguous matrix.

    The parquet ``embedding`` column is ``list<float>``; flatten via
    the pyarrow chunked API and reshape so we never materialise a
    Python nested-list copy.
    """
    table = pq.read_table(bundle / "reference_embeddings.parquet")
    accs = [str(a) for a in table.column("accession").to_pylist()]

    embedding_col = table.column("embedding")
    flat = embedding_col.combine_chunks().flatten().to_numpy(zero_copy_only=False)
    n = len(accs)
    if n == 0:
        return accs, np.empty((0, 0), dtype=np.float32)
    if flat.size == 0 or flat.size % n != 0:
        raise ValueError(
            f"Reference embeddings parquet inconsistent: {flat.size} flat values vs {n} rows.",
        )
    dim = flat.size // n
    return accs, flat.reshape(n, dim).astype(np.float32, copy=False)


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
    """Load the legacy single-booster blob if present."""
    booster_path = bundle / "reranker.txt"
    if not booster_path.exists():
        return None
    return booster_path.read_bytes()


def _load_boosters_by_aspect(bundle: Path) -> dict[str, bytes]:
    """Load per-aspect boosters from ``<bundle>/reranker/{F,P,C}.txt``.

    Returns an empty dict when ``reranker/`` is absent; the caller
    falls back to the legacy single-booster path or the no-rerank
    baseline (1 - distance).
    """
    reranker_dir = bundle / "reranker"
    if not reranker_dir.is_dir():
        return {}
    out: dict[str, bytes] = {}
    for aspect in ("F", "P", "C"):
        path = reranker_dir / f"{aspect}.txt"
        if path.exists():
            out[aspect] = path.read_bytes()
    return out


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


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the protea-predict argument parser.

    Exposed at module top-level so tests can exercise the surface
    without importing the heavy ``torch``/``transformers`` deps that
    the runtime path pulls in.
    """
    parser = argparse.ArgumentParser(
        prog="protea-predict",
        description=(
            "PROTEA inference CLI. Embeds queries with ProtT5, then runs "
            "KNN + v6 features + LightGBM reranker against a frozen "
            "reference bundle. FASTA in, TSV out."
        ),
    )
    parser.add_argument("--query_file", "-q", required=True, help="FASTA of query sequences.")
    parser.add_argument(
        "--frozen_data_dir",
        required=True,
        help="Bind-mounted bundle directory (manifest.json + parquets + reranker/).",
    )
    parser.add_argument(
        "--output",
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
        help="Skip the LightGBM reranker even if a booster ships in the bundle.",
    )
    parser.add_argument(
        "--self_prior",
        action="store_true",
        help=(
            "Inject the GOA self-prior: each query target's OWN t0 "
            "non-experimental annotation (from the frozen bundle), scored "
            "confidently and max-combined with neighbour transfer. Default off."
        ),
    )
    parser.add_argument(
        "--self_prior_score",
        type=float,
        default=1.0,
        help="Score for own-annotation self-prior candidates (default 1.0).",
    )
    parser.add_argument(
        "--self_prior_neighbour_scale",
        type=float,
        default=0.95,
        help=(
            "Multiplier on neighbour-transfer scores before combining with the "
            "self-prior (default 0.95) so the self-prior dominates the band."
        ),
    )
    parser.add_argument(
        "--model_dir",
        default=os.environ.get("HF_CACHE"),
        help="HuggingFace cache dir for ProtT5 (default: $HF_CACHE).",
    )
    return parser


def write_predictions_tsv(
    predictions: list[dict[str, Any]],
    go_id_map: dict[int, str],
    out_path: str,
) -> int:
    """Emit predictions to a TSV (gzipped if ``out_path`` ends in .gz).

    Returns the number of rows written.
    """
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
    return n_rows


def main(argv: list[str] | None = None) -> None:
    # Imports deferred so ``build_arg_parser`` and the bundle helpers
    # stay importable in environments without torch/transformers
    # (tests, ahead-of-time validation, container linting).
    from protea_method.anc2vec import Anc2VecIndex
    from protea_method.pipeline import PredictConfig, predict
    from protea_method.reranker import load_from_bytes
    from prott5_encoder import embed_sequences, parse_fasta

    args = build_arg_parser().parse_args(argv)

    bundle = Path(args.frozen_data_dir)
    if not bundle.is_dir():
        print(f"[protea-predict] frozen_data_dir not a directory: {bundle}", file=sys.stderr)
        sys.exit(1)

    manifest = _load_manifest(bundle)
    print(
        f"[protea-predict] bundle: cutoff={manifest.get('cutoff_version', '?')} "
        f"schema_sha={manifest.get('feature_schema_sha', '?')[:12]}",
    )

    print(f"[protea-predict] reading queries: {args.query_file}")
    query_seqs = parse_fasta(args.query_file)
    if not query_seqs:
        print("[protea-predict] no query sequences parsed; aborting.", file=sys.stderr)
        sys.exit(2)
    query_order = list(query_seqs.keys())
    print(f"[protea-predict] {len(query_seqs)} query sequences")

    print("[protea-predict] embedding queries with ProtT5 (mean-pool)")
    query_embeddings_by_acc = embed_sequences(query_seqs, cache_dir=args.model_dir)
    Q, kept_q = _stack_query_embeddings(query_embeddings_by_acc, query_order)
    if Q.size == 0:
        print("[protea-predict] all queries failed to embed; aborting.", file=sys.stderr)
        sys.exit(3)

    print("[protea-predict] loading frozen bundle")
    ref_accs, R = _load_refs(bundle)
    annotations = _load_annotations(bundle)
    go_id_map, go_aspect_map = _load_go_metadata(bundle)
    pca_state = _load_pca_state(bundle) if not args.no_v6 else None
    anc_idx = (
        Anc2VecIndex(bundle / "anc2vec.npz")
        if (bundle / "anc2vec.npz").exists() and not args.no_v6
        else None
    )
    boosters_by_aspect: dict[str, object] = {}
    booster = None
    if not args.no_reranker:
        per_aspect_bytes = _load_boosters_by_aspect(bundle)
        if per_aspect_bytes:
            boosters_by_aspect = {
                aspect: load_from_bytes(blob) for aspect, blob in per_aspect_bytes.items()
            }
        else:
            booster_bytes = _load_booster_bytes(bundle)
            if booster_bytes is not None:
                booster = load_from_bytes(booster_bytes)
    rerank_summary = (
        f"per-aspect={sorted(boosters_by_aspect)}"
        if boosters_by_aspect
        else f"single={booster is not None}"
    )
    print(
        f"[protea-predict] refs={len(ref_accs)} "
        f"annotations={sum(len(v) for v in annotations.values())} "
        f"go_terms={len(go_id_map)} pca={pca_state is not None} "
        f"anc2vec={anc_idx is not None} reranker={rerank_summary}",
    )

    config = PredictConfig(
        k=args.k,
        metric=args.metric,
        backend=args.backend,
        aspect_separated=args.aspect_separated,
        compute_v6_features=not args.no_v6 and anc_idx is not None,
        compute_taxonomy=False,
        pre_normalized=False,
    )
    print(f"[protea-predict] running predict (config={config})")
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
        boosters_by_aspect=boosters_by_aspect or None,
        anc_idx=anc_idx,
    )
    print(f"[protea-predict] {len(predictions)} prediction rows")

    if args.self_prior:
        from self_prior import build_self_prior_rows, combine_with_self_prior

        self_rows = build_self_prior_rows(
            annotations, kept_q, self_score=args.self_prior_score,
        )
        n_targets = len({r["protein_accession"] for r in self_rows})
        print(
            f"[protea-predict] self-prior: {len(self_rows)} own-annotation "
            f"candidates over {n_targets}/{len(kept_q)} query targets "
            f"(neighbour_scale={args.self_prior_neighbour_scale})",
        )
        predictions = combine_with_self_prior(
            predictions,
            self_rows,
            neighbour_scale=args.self_prior_neighbour_scale,
            score_of=_score_for_output,
        )
        print(f"[protea-predict] {len(predictions)} rows after self-prior combine")

    n_rows = write_predictions_tsv(predictions, go_id_map, args.output)
    print(f"[protea-predict] wrote {n_rows} predictions to {args.output}")


if __name__ == "__main__":
    main()
