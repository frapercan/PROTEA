"""Ensemble driver for the protea-knn-8plm LAFA container.

For each PLM in the ensemble:

1. Embed the queries (mean-pool over the residue axis).
2. Load the per-PLM frozen bundle (one ``reference_embeddings.parquet``
   + the shared annotations / GO metadata).
3. Run an aspect-separated KNN search (one neighbour set per GO aspect).
4. Emit per-PLM ``(query, go_term, distance)`` candidates.

The driver then combines the eight candidate sets by score-level mean
aggregation (see :func:`aggregate_predictions` for the rationale) and
writes a single TSV in the same format as the v1 baseline. No v6
features, no learned reranker (those ship in ``protea-v18`` per
ADR-D23).

Per-PLM helpers are split out so the orchestration step
(:func:`run_ensemble`) stays under the smell budget.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

# Importable in unit tests without paying for torch/transformers.
from apps.lafa_knn_8plm.plm_encoders import (
    PLM_SPECS,
    EncoderFn,
    build_registry,
    list_plm_keys,
)

#: Allowed aggregation strategies. ``mean`` is the canonical
#: late-fusion baseline; ``max`` mirrors the score-fusion variant from
#: the lab grid. ``mean`` is the default per the F-LAFA.2 acceptance
#: criterion ("8 PLMs averaged at score level") and per ADR-D23.
AGGREGATIONS = ("mean", "max")


@dataclass(frozen=True)
class EnsembleConfig:
    """Static knobs for one ``run_ensemble`` invocation."""

    k: int = 5
    metric: str = "cosine"
    aggregation: str = "mean"
    plms: tuple[str, ...] = tuple(list_plm_keys())

    def __post_init__(self) -> None:
        if self.aggregation not in AGGREGATIONS:
            raise ValueError(
                f"unknown aggregation: {self.aggregation!r} (allowed: {AGGREGATIONS})"
            )
        if self.metric not in ("cosine", "l2"):
            raise ValueError(f"unknown metric: {self.metric!r}")
        if self.k < 1:
            raise ValueError("k must be >= 1")
        for plm in self.plms:
            if plm not in {spec.key for spec in PLM_SPECS}:
                raise ValueError(f"unknown PLM key in --plms: {plm!r}")


# --- bundle helpers (mirror apps/method_runtime/protea_predict.py) ----


def _open_output(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", newline="")
    return open(path, "w", newline="")


def load_manifest(bundle: Path) -> dict[str, Any]:
    """Load ``<bundle>/manifest.json``."""
    return json.loads((bundle / "manifest.json").read_text())


def load_plm_refs(bundle: Path, plm_key: str) -> tuple[list[str], np.ndarray]:
    """Load (accessions, embeddings) for one PLM from the bundle.

    The 8plm bundle layout puts per-PLM matrices under
    ``<bundle>/plms/<plm_key>/reference_embeddings.parquet``. A legacy
    layout (single ``reference_embeddings.parquet`` at the bundle root)
    is also accepted for one-PLM smoke runs.
    """
    plm_path = bundle / "plms" / plm_key / "reference_embeddings.parquet"
    root_path = bundle / "reference_embeddings.parquet"
    parquet_path = plm_path if plm_path.exists() else root_path
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"missing reference embeddings for PLM {plm_key!r}: "
            f"tried {plm_path} then {root_path}"
        )
    table = pq.read_table(parquet_path)
    accs = [str(a) for a in table.column("accession").to_pylist()]
    flat = table.column("embedding").combine_chunks().flatten().to_numpy(zero_copy_only=False)
    n = len(accs)
    if n == 0:
        return accs, np.empty((0, 0), dtype=np.float32)
    if flat.size == 0 or flat.size % n != 0:
        raise ValueError(
            f"per-PLM embeddings parquet inconsistent for {plm_key}: "
            f"{flat.size} values vs {n} rows"
        )
    dim = flat.size // n
    return accs, flat.reshape(n, dim).astype(np.float32, copy=False)


def load_annotations(bundle: Path) -> dict[str, list[dict[str, Any]]]:
    """Load ``<bundle>/reference_annotations.parquet`` grouped by accession."""
    table = pq.read_table(bundle / "reference_annotations.parquet")
    rows = table.to_pylist()
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row["accession"]), []).append(
            {k: v for k, v in row.items() if k != "accession"},
        )
    return out


def load_go_metadata(bundle: Path) -> tuple[dict[int, str], dict[int, str]]:
    """Return ``(go_term_id -> go_id, go_term_id -> aspect)``."""
    table = pq.read_table(bundle / "go_term_metadata.parquet")
    rows = table.to_pylist()
    go_id_map: dict[int, str] = {}
    go_aspect_map: dict[int, str] = {}
    for row in rows:
        gid = int(row["go_term_id"])
        go_id_map[gid] = str(row["go_id"])
        go_aspect_map[gid] = str(row.get("aspect") or "")
    return go_id_map, go_aspect_map


# --- per-PLM helpers --------------------------------------------------


def stack_queries(
    embeddings: dict[str, np.ndarray],
    order: list[str],
) -> tuple[np.ndarray, list[str]]:
    """Stack one PLM's query embeddings in FASTA order."""
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


def run_plm_knn(
    plm_key: str,
    query_seqs: dict[str, str],
    query_order: list[str],
    bundle: Path,
    *,
    encoder: EncoderFn,
    annotations: dict[str, list[dict[str, Any]]],
    go_aspect_map: dict[int, str],
    config: EnsembleConfig,
    cache_dir: str | None,
    device: str | None,
) -> list[dict[str, Any]]:
    """Embed + KNN + GO transfer for one PLM. Returns prediction rows.

    Each row is ``{"protein_accession", "go_term_id", "min_distance",
    "plm"}``. ``min_distance`` is the cosine (or L2) distance to the
    closest neighbour that voted for the GO term inside the candidate's
    aspect; ``plm`` is the encoder key (preserved so the aggregator can
    debug per-PLM contributions).
    """
    from protea_method.pipeline import (  # type: ignore[import-not-found]
        PredictConfig,
        predict,
    )

    query_embeddings = encoder(query_seqs, cache_dir, device)
    query_matrix, kept = stack_queries(query_embeddings, query_order)
    if query_matrix.size == 0:
        return []

    ref_accs, ref_matrix = load_plm_refs(bundle, plm_key)
    plm_config = PredictConfig(
        k=config.k,
        metric=config.metric,
        backend="numpy",
        aspect_separated=True,
        compute_v6_features=False,
        compute_taxonomy=False,
        pre_normalized=False,
    )
    rows = predict(
        query_accessions=kept,
        query_embeddings=query_matrix,
        reference_accessions=ref_accs,
        reference_embeddings=ref_matrix,
        annotations=annotations,
        go_id_map={},  # unused: aggregation works on go_term_id, mapping is applied at write time
        go_aspect_map=go_aspect_map,
        config=plm_config,
        pca_state=None,
        booster=None,
        boosters_by_aspect=None,
        anc_idx=None,
    )
    return [{**row, "plm": plm_key} for row in rows]


def _row_score(pred: dict[str, Any]) -> float:
    """Convert one per-PLM prediction row to a ``[0, 1]`` score.

    Uses ``max(0, 1 - min_distance)``; the LightGBM reranker is not in
    the v2 baseline (and would be applied per-aspect inside
    ``protea-v18``, not here).
    """
    distance = float(pred.get("min_distance", pred.get("distance", 1.0)))
    return max(0.0, 1.0 - distance)


# --- ensemble aggregation --------------------------------------------


def aggregate_predictions(
    per_plm_rows: Iterable[dict[str, Any]],
    *,
    aggregation: str,
    n_plms: int,
) -> list[dict[str, Any]]:
    """Combine per-PLM ``(query, go_term)`` candidates into one ranked set.

    For each ``(protein_accession, go_term_id)`` key we collect the
    per-PLM scores. ``mean`` averages over all 8 PLMs (missing PLMs
    contribute 0, so a term seen by only one model is penalised by a
    factor of ``1 / n_plms``); ``max`` keeps the strongest single-PLM
    vote.

    The mean default mirrors the F-LAFA.2 acceptance criterion
    ("averaged at score level") and ADR-D23; max remains available for
    ablation runs without rebuilding the image.
    """
    if aggregation not in AGGREGATIONS:
        raise ValueError(f"unknown aggregation: {aggregation!r}")

    bucket: dict[tuple[str, int], list[float]] = {}
    plms_seen: dict[tuple[str, int], set[str]] = {}
    for row in per_plm_rows:
        key = (str(row["protein_accession"]), int(row["go_term_id"]))
        bucket.setdefault(key, []).append(_row_score(row))
        plms_seen.setdefault(key, set()).add(str(row.get("plm", "")))

    out: list[dict[str, Any]] = []
    for (acc, gid), scores in bucket.items():
        if aggregation == "mean":
            # Divide by the full ensemble size, not len(scores), so a
            # GO term seen by 1 PLM scores 8x lower than one seen by all.
            agg_score = float(sum(scores)) / max(1, n_plms)
        else:  # "max"
            agg_score = max(scores)
        out.append(
            {
                "protein_accession": acc,
                "go_term_id": gid,
                "ensemble_score": agg_score,
                "n_plms_voted": len(plms_seen[(acc, gid)]),
            }
        )

    # Stable ordering: descending score, then accession, then go_term_id.
    out.sort(key=lambda r: (-r["ensemble_score"], r["protein_accession"], r["go_term_id"]))
    return out


# --- output ----------------------------------------------------------


def write_predictions_tsv(
    predictions: list[dict[str, Any]],
    go_id_map: dict[int, str],
    out_path: str,
) -> int:
    """Emit ``<query>\\t<go_id>\\t<ensemble_score>`` rows (gzipped on .gz)."""
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
            score = float(pred["ensemble_score"])
            writer.writerow([pred["protein_accession"], go_id, f"{score:.4f}"])
            n_rows += 1
    return n_rows


# --- entry points ----------------------------------------------------


def run_ensemble(
    *,
    query_seqs: dict[str, str],
    query_order: list[str],
    bundle: Path,
    config: EnsembleConfig,
    encoders: dict[str, EncoderFn],
    cache_dir: str | None,
    device: str | None,
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    """Top-level orchestration: returns (aggregated_predictions, go_id_map)."""
    annotations = load_annotations(bundle)
    go_id_map, go_aspect_map = load_go_metadata(bundle)

    per_plm: list[dict[str, Any]] = []
    for plm_key in config.plms:
        encoder = encoders.get(plm_key)
        if encoder is None:
            print(f"[ensemble] skipping {plm_key}: encoder missing from registry", file=sys.stderr)
            continue
        print(f"[ensemble] running PLM {plm_key}")
        rows = run_plm_knn(
            plm_key,
            query_seqs,
            query_order,
            bundle,
            encoder=encoder,
            annotations=annotations,
            go_aspect_map=go_aspect_map,
            config=config,
            cache_dir=cache_dir,
            device=device,
        )
        print(f"[ensemble] PLM {plm_key}: {len(rows)} candidate rows")
        per_plm.extend(rows)

    aggregated = aggregate_predictions(
        per_plm,
        aggregation=config.aggregation,
        n_plms=len(config.plms),
    )
    return aggregated, go_id_map


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="protea-knn-8plm",
        description=(
            "PROTEA F-LAFA v2 ensemble submission. Embeds queries with 8 PLMs, "
            "runs aspect-separated KNN per PLM against a frozen reference "
            "bundle, then aggregates candidates by score-level mean (default) "
            "or max. FASTA in, TSV out."
        ),
    )
    parser.add_argument("--query_file", "-q", required=True, help="FASTA of query sequences.")
    parser.add_argument(
        "--frozen_data_dir",
        required=True,
        help=(
            "Bind-mounted bundle directory (per-PLM matrices under plms/<key>/ "
            "and shared annotations / GO metadata at the bundle root)."
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output TSV: <query>\\t<go_id>\\t<score>. Gzipped if suffix is .gz.",
    )
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--metric", default="cosine", choices=["cosine", "l2"])
    parser.add_argument(
        "--aggregation",
        default="mean",
        choices=list(AGGREGATIONS),
        help="Score-level fusion across PLMs (default: mean, per ADR-D23).",
    )
    parser.add_argument(
        "--plms",
        default=",".join(list_plm_keys()),
        help=(
            "Comma-separated subset of ensemble members. Default: every PLM "
            "in the canonical 8-PLM line-up."
        ),
    )
    parser.add_argument(
        "--model_dir",
        default=os.environ.get("HF_CACHE"),
        help="HuggingFace cache dir (default: $HF_CACHE).",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Override torch device (default: cuda:0 if available, else cpu).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)

    bundle = Path(args.frozen_data_dir)
    if not bundle.is_dir():
        print(f"[protea-knn-8plm] frozen_data_dir not a directory: {bundle}", file=sys.stderr)
        sys.exit(1)

    manifest = load_manifest(bundle)
    print(
        f"[protea-knn-8plm] bundle: cutoff={manifest.get('cutoff_version', '?')} "
        f"schema_sha={manifest.get('feature_schema_sha', '?')[:12]}",
    )

    from prott5_encoder import parse_fasta  # type: ignore[import-not-found]

    query_seqs = parse_fasta(args.query_file)
    if not query_seqs:
        print("[protea-knn-8plm] no query sequences parsed; aborting.", file=sys.stderr)
        sys.exit(2)
    query_order = list(query_seqs.keys())
    print(f"[protea-knn-8plm] {len(query_seqs)} query sequences")

    selected = tuple(s for s in (p.strip() for p in args.plms.split(",")) if s)
    config = EnsembleConfig(
        k=args.k,
        metric=args.metric,
        aggregation=args.aggregation,
        plms=selected,
    )
    print(
        f"[protea-knn-8plm] config: k={config.k} metric={config.metric} "
        f"aggregation={config.aggregation} plms={list(config.plms)}",
    )

    encoders = build_registry()
    aggregated, go_id_map = run_ensemble(
        query_seqs=query_seqs,
        query_order=query_order,
        bundle=bundle,
        config=config,
        encoders=encoders,
        cache_dir=args.model_dir,
        device=args.device,
    )
    print(f"[protea-knn-8plm] {len(aggregated)} aggregated prediction rows")

    n_rows = write_predictions_tsv(aggregated, go_id_map, args.output)
    print(f"[protea-knn-8plm] wrote {n_rows} predictions to {args.output}")


if __name__ == "__main__":
    main()
