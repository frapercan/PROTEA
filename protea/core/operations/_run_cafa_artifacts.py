"""Artifact-IO helpers extracted from ``run_cafa_evaluation``.

These pure functions handle downloading inputs (OBO, IA TSV), writing
ground-truth / prediction TSVs that ``cafaeval`` consumes, and parsing
the metrics it produces. Module-level so the operation file
(``run_cafa_evaluation.py``) can stay focused on the orchestrator and
shrink toward the master-plan v3.2 §3 LOC ceiling.
"""

from __future__ import annotations

import gzip
import shutil
import uuid
from dataclasses import dataclass
from typing import Any

import requests
from sqlalchemy.orm import Session

from protea.core.operations._run_cafa_helpers import (
    _NS_LABELS,
    _patch_query_known_features,
    _record_from_pred,
)
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


@dataclass(frozen=True)
class WritePredictionsContext:
    """Shared inputs for the per-setting prediction TSV writers.

    Bundles the four query-shaped fields (``pred_set_id``,
    ``delta_proteins``, ``max_distance``) plus the destination
    ``path`` consumed by all three artifact writers
    (``write_predictions``, ``write_predictions_reranked``,
    ``write_predictions_per_aspect``). The session, scoring
    snapshot, and reranker bundle stay outside ctx because they
    encode IO / scoring strategy, not query parameters.
    """

    pred_set_id: uuid.UUID
    delta_proteins: set[str]
    max_distance: float | None
    path: str


def download_obo(url: str, dest: str) -> None:
    """Download OBO file to dest, decompressing gzip if needed."""
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    if url.endswith(".gz"):
        with open(dest, "wb") as f:
            f.write(gzip.decompress(resp.content))
    else:
        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)


def download_tsv(url: str, dest: str) -> None:
    """Copy or download a plain-text TSV file (gzip-transparent) to dest.

    Accepts both HTTP(S) URLs and local filesystem paths (absolute or
    ``file://`` scheme).  Local paths are resolved without any network
    request, which is useful during development when the IA file lives
    inside the repository (``data/benchmarks/IA_cafa6.tsv``) and
    ``ia_url`` is set to its absolute path.  Once the file is pushed to
    GitHub the URL can be switched to the raw.githubusercontent.com
    address and the same code path handles it transparently.
    """
    local_path: str | None = None
    if url.startswith("file://"):
        local_path = url[len("file://") :]
    elif url.startswith("/"):
        local_path = url

    if local_path is not None:
        if url.endswith(".gz"):
            with gzip.open(local_path, "rb") as src, open(dest, "wb") as f:
                shutil.copyfileobj(src, f)
        else:
            shutil.copy2(local_path, dest)
        return

    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    if url.endswith(".gz"):
        with open(dest, "wb") as f:
            f.write(gzip.decompress(resp.content))
    else:
        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)


def write_gt(annotations: dict[str, set[str]], path: str) -> None:
    """Write {protein: {go_id}} to a 2-column TSV (no header)."""
    with open(path, "w") as f:
        for protein in sorted(annotations):
            for go_id in sorted(annotations[protein]):
                f.write(f"{protein}\t{go_id}\n")


def _score_unranked_pred(
    pred: GOPrediction,
    scoring_config: ScoringConfig | None,
) -> float:
    """Score one prediction with a ``ScoringConfig`` or the distance fallback."""
    if scoring_config is None:
        return max(0.0, 1.0 - (pred.distance or 0.0) / 2.0)
    pred_dict = {
        "distance": pred.distance,
        "identity_nw": pred.identity_nw,
        "identity_sw": pred.identity_sw,
        "evidence_code": pred.evidence_code,
        "taxonomic_distance": pred.taxonomic_distance,
        "neighbor_vote_fraction": pred.neighbor_vote_fraction,
    }
    return compute_score(pred_dict, scoring_config)


def write_predictions(
    session: Session,
    ctx: WritePredictionsContext,
    *,
    scoring_config: ScoringConfig | None = None,
    reranker_model_str: str | None = None,
    reranker_cat_codes: dict[str, list[str]] | None = None,
    known_gos: dict[str, set[str]] | None = None,
) -> None:
    """Write CAFA-format predictions (protein\\tgo_id\\tscore) for delta proteins.

    Scoring priority:
      1. If ``reranker_model_str`` is provided, apply the LightGBM model to
         all predictions and use re-ranker probabilities as scores.
      2. If a ``ScoringConfig`` is provided, compute scores via ``compute_score()``.
      3. Otherwise fall back to ``1 - cosine_distance / 2``.

    ``known_gos`` carries the query's pre-cutoff annotations (LK / PK
    settings); for NK it must stay ``None``.
    """
    if reranker_model_str is not None:
        write_predictions_reranked(
            session,
            ctx,
            reranker_model_str=reranker_model_str,
            reranker_cat_codes=reranker_cat_codes,
            known_gos=known_gos,
        )
        return
    q = (
        session.query(GOPrediction, GOTerm)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .filter(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        q = q.filter(GOPrediction.distance <= ctx.max_distance)
    q = q.order_by(GOPrediction.protein_accession, GOTerm.go_id, GOPrediction.distance)
    seen: set[tuple[str, str]] = set()
    with open(ctx.path, "w") as f:
        for pred, gt in q.yield_per(1000):
            key = (pred.protein_accession, gt.go_id)
            if key in seen:
                continue
            seen.add(key)
            score = _score_unranked_pred(pred, scoring_config)
            f.write(f"{pred.protein_accession}\t{gt.go_id}\t{score:.4f}\n")


def write_predictions_reranked(
    session: Session,
    ctx: WritePredictionsContext,
    *,
    reranker_model_str: str,
    reranker_cat_codes: dict[str, list[str]] | None = None,
    known_gos: dict[str, set[str]] | None = None,
) -> None:
    """Write CAFA-format predictions using LightGBM re-ranker scores."""
    import pandas as pd

    from protea.core.reranker import model_from_string
    from protea.core.reranker import predict as reranker_predict

    q = (
        session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .filter(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        q = q.filter(GOPrediction.distance <= ctx.max_distance)

    records: list[dict[str, Any]] = [
        _record_from_pred(pred, go_id, aspect=aspect)
        for pred, go_id, aspect in q.yield_per(5000)
    ]

    if not records:
        with open(ctx.path, "w") as f:
            pass
        return

    df = pd.DataFrame(records)
    if known_gos:
        _patch_query_known_features(df, known_gos)
    model = model_from_string(reranker_model_str)
    scores = reranker_predict(model, df, categorical_codes=reranker_cat_codes)

    df["score"] = scores
    df = df.sort_values("score", ascending=False).drop_duplicates(
        subset=["protein_accession", "go_id"],
        keep="first",
    )

    with open(ctx.path, "w") as f:
        for _, row in df.iterrows():
            f.write(f"{row['protein_accession']}\t{row['go_id']}\t{row['score']:.4f}\n")


def _load_aspect_records(
    session: Session,
    ctx: WritePredictionsContext,
) -> list[dict[str, Any]]:
    """Stream the (pred, go_id, aspect) rows for ``ctx.delta_proteins`` into records."""
    q = (
        session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .filter(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        q = q.filter(GOPrediction.distance <= ctx.max_distance)
    return [_record_from_pred(pred, go_id, aspect) for pred, go_id, aspect in q.yield_per(5000)]


def _apply_per_aspect_scores(
    df: Any,
    aspect_models: dict[str, dict[str, Any]],
) -> None:
    """Mutate ``df['score']`` by aspect-keyed LightGBM model; fall back to 1 - d/2."""
    from protea.core.reranker import model_from_string
    from protea.core.reranker import predict as reranker_predict

    df["score"] = 0.0
    for aspect_char, bundle in aspect_models.items():
        mask = df["aspect"] == aspect_char
        if not mask.any():
            continue
        model = model_from_string(bundle["model"])
        df.loc[mask, "score"] = reranker_predict(
            model,
            df.loc[mask],
            categorical_codes=bundle.get("cat_codes"),
        )
    fallback_mask = ~df["aspect"].isin(set(aspect_models.keys()))
    if fallback_mask.any():
        df.loc[fallback_mask, "score"] = df.loc[fallback_mask, "distance"].apply(
            lambda d: max(0.0, 1.0 - (d or 0.0) / 2.0)
        )


def write_predictions_per_aspect(
    session: Session,
    ctx: WritePredictionsContext,
    *,
    aspect_models: dict[str, dict[str, Any]],
    known_gos: dict[str, set[str]] | None = None,
) -> None:
    """Write CAFA-format predictions applying per-aspect LightGBM models.

    ``aspect_models`` maps GO aspect char (P/F/C) to ``{"model": str,
    "cat_codes": dict|None}`` bundles. Predictions whose aspect has no
    model fall back to ``1 - distance/2``. ``known_gos`` carries the
    query's pre-cutoff annotations (LK / PK settings); must be ``None``
    for NK.
    """
    import pandas as pd

    records = _load_aspect_records(session, ctx)
    if not records:
        with open(ctx.path, "w") as f:
            pass
        return
    df = pd.DataFrame(records)
    if known_gos:
        _patch_query_known_features(df, known_gos)
    _apply_per_aspect_scores(df, aspect_models)
    df = df.sort_values("score", ascending=False).drop_duplicates(
        subset=["protein_accession", "go_id"],
        keep="first",
    )
    with open(ctx.path, "w") as f:
        for _, row in df.iterrows():
            f.write(f"{row['protein_accession']}\t{row['go_id']}\t{row['score']:.4f}\n")


def parse_results(dfs_best: dict) -> dict[str, Any]:
    """Extract per-namespace Fmax metrics from cafaeval dfs_best.

    The unweighted metrics are read from ``dfs_best["f"]`` (one row
    per namespace at the threshold optimising the protein-mean
    Fmax). When an IA file was supplied to cafaeval, the IA-weighted
    equivalents land in ``dfs_best["f_w"]`` (best by IA-weighted
    Fmax), the micro-averaged Fmax in ``dfs_best["f_micro"]`` /
    ``dfs_best["f_micro_w"]``, and the minimum semantic distance in
    ``dfs_best["s"]`` (best by lowest S, the IA-weighted misinformation
    /remaining-uncertainty distance); these are surfaced here as the
    ``_w`` / ``_micro`` / ``_micro_w`` / ``s_min`` keys so chapter-6
    tables can pull them without re-reading the per-tier TSV artifacts.
    ``s_min`` is the canonical CAFA S_min and, like ``f_micro_w``,
    requires a real IA file to be meaningful (it is IA-weighted).
    """
    ns_results: dict[str, Any] = {}

    df_f = dfs_best.get("f")
    if df_f is None or df_f.empty:
        return ns_results

    df_f = df_f.reset_index()
    for _, row in df_f.iterrows():
        ns_long = str(row.get("ns", ""))
        ns = _NS_LABELS.get(ns_long)
        if ns is None:
            continue
        ns_results[ns] = {
            "fmax": round(float(row.get("f", 0)), 4),
            "precision": round(float(row.get("pr", 0)), 4),
            "recall": round(float(row.get("rc", 0)), 4),
            "tau": round(float(row.get("tau", 0)), 4),
            "coverage": round(float(row.get("cov_max", row.get("cov", 0))), 4),
            "n_proteins": int(row.get("n", 0)) if "n" in row else None,
        }

    for key, fmt in (
        ("f_w", "fmax_w"),
        ("f_micro", "f_micro"),
        ("f_micro_w", "f_micro_w"),
        ("s", "s_min"),
    ):
        df_extra = dfs_best.get(key)
        if df_extra is None or df_extra.empty:
            continue
        df_extra = df_extra.reset_index()
        col = key
        for _, row in df_extra.iterrows():
            ns_long = str(row.get("ns", ""))
            ns = _NS_LABELS.get(ns_long)
            if ns is None or ns not in ns_results:
                continue
            ns_results[ns][fmt] = round(float(row.get(col, 0)), 4)

    return ns_results
