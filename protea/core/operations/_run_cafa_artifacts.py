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

import numpy as np
import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.operations import _pred_base_cache
from protea.core.operations._run_cafa_helpers import (
    _NS_LABELS,
    _patch_query_known_features,
    _record_from_pred,
)
from protea.core.row_alignment import assert_unique_key
from protea.core.scoring import compute_score, evidence_weight
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_EVIDENCE_WEIGHTED,
    IA_PRIOR_SOURCE_IA,
    ScoringConfig,
)

# Columns the baseline (no-reranker) scorer reads off each prediction. Fetched
# as a Core columnar query (never 1.2M ORM objects), deduped to the
# lowest-distance row per (protein, go_id), then vectorised.
_BASE_SCORE_COLS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "distance",
    "identity_nw",
    "identity_sw",
    "evidence_code",
    "taxonomic_distance",
    "neighbor_vote_fraction",
    # --- A-SCORE rich axes (coverage A3 / ref-density F / anc2vec G / IA-prior E) ---
    "alignment_length_nw",
    "gaps_pct_nw",
    "alignment_length_sw",
    "gaps_pct_sw",
    "length_query",
    "ref_annotation_density",
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "go_term_frequency",
)


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
        # A-SCORE rich axes (coverage A3 / ref-density F / anc2vec G / IA-prior E)
        "alignment_length_nw": getattr(pred, "alignment_length_nw", None),
        "gaps_pct_nw": getattr(pred, "gaps_pct_nw", None),
        "alignment_length_sw": getattr(pred, "alignment_length_sw", None),
        "gaps_pct_sw": getattr(pred, "gaps_pct_sw", None),
        "length_query": getattr(pred, "length_query", None),
        "ref_annotation_density": getattr(pred, "ref_annotation_density", None),
        "anc2vec_neighbor_cos": getattr(pred, "anc2vec_neighbor_cos", None),
        "anc2vec_neighbor_maxcos": getattr(pred, "anc2vec_neighbor_maxcos", None),
        "go_term_frequency": getattr(pred, "go_term_frequency", None),
    }
    return compute_score(pred_dict, scoring_config)


def _score_bundle_df(df: Any, bundle: dict[str, Any]) -> Any:
    """Score ``df`` with a reranker ``bundle`` (``{model, cat_codes, universal}``).

    A bundle carrying a non-``None`` ``universal`` block (a universal pooled,
    K-augmented booster) is scored through
    :func:`protea.core._universal_reranker.score_universal`: its model carries
    injected ``plm_id`` / ``k_context`` feature columns the generic
    ``reranker_predict`` cannot encode. Per-cell bundles keep the generic
    ``reranker_predict`` path unchanged (F-RERANK-UNIVERSAL eval wiring).
    """
    from protea.core.reranker import model_from_string, prepare_reranker_frame
    from protea.core.reranker import predict as reranker_predict

    model = model_from_string(bundle["model"])
    universal = bundle.get("universal")
    if universal is not None:
        from protea.core._universal_reranker import score_universal

        return score_universal(
            model,
            df,
            categorical_codes=universal["categorical_codes"],
            plm_id=universal["plm_id"],
            k_context=universal["k_context"],
        )
    # Derive ad-hoc reranker features (aspect_code -> INT) and guard any
    # ungoverned object column so a per-category booster that splits on
    # aspect_code scores cleanly instead of tripping LightGBM's opaque
    # "pandas dtypes must be int, float or bool" error in run_cafa_evaluation.
    df = prepare_reranker_frame(model, df)
    return reranker_predict(model, df, categorical_codes=bundle.get("cat_codes"))


def write_predictions(
    session: Session,
    ctx: WritePredictionsContext,
    *,
    scoring_config: ScoringConfig | None = None,
    reranker_bundle: dict[str, Any] | None = None,
    known_gos: dict[str, set[str]] | None = None,
) -> None:
    """Write CAFA-format predictions (protein\\tgo_id\\tscore) for delta proteins.

    Scoring priority:
      1. If ``reranker_bundle`` is provided, apply the LightGBM model to all
         predictions and use re-ranker probabilities as scores. The bundle is
         ``{"model": str, "cat_codes": dict|None, "universal": dict|None}``; a
         non-``None`` ``universal`` block routes scoring through
         :func:`score_universal` (F-RERANK-UNIVERSAL eval wiring).
      2. If a ``ScoringConfig`` is provided, compute scores via ``compute_score()``.
      3. Otherwise fall back to ``1 - cosine_distance / 2``.

    ``known_gos`` carries the query's pre-cutoff annotations (LK / PK
    settings); for NK it must stay ``None``.
    """
    if reranker_bundle is not None:
        write_predictions_reranked(
            session,
            ctx,
            reranker_bundle=reranker_bundle,
            known_gos=known_gos,
        )
        return
    base = _pred_base_cache.load_or_build_base(
        ctx.pred_set_id,
        ctx.max_distance,
        ctx.delta_proteins,
        count_fn=lambda: _count_base_rows(session, ctx),
        build_fn=lambda: _build_base_frame(session, ctx),
    )
    _write_scored_base(base, scoring_config, ctx.path)


def _base_select(ctx: WritePredictionsContext) -> Any:
    """Core columnar SELECT of the baseline scoring inputs for ``ctx``."""
    stmt = (
        select(
            GOPrediction.protein_accession,
            GOTerm.go_id,
            GOPrediction.distance,
            GOPrediction.identity_nw,
            GOPrediction.identity_sw,
            GOPrediction.evidence_code,
            GOPrediction.taxonomic_distance,
            GOPrediction.neighbor_vote_fraction,
            GOPrediction.alignment_length_nw,
            GOPrediction.gaps_pct_nw,
            GOPrediction.alignment_length_sw,
            GOPrediction.gaps_pct_sw,
            GOPrediction.length_query,
            GOPrediction.ref_annotation_density,
            GOPrediction.anc2vec_neighbor_cos,
            GOPrediction.anc2vec_neighbor_maxcos,
            GOPrediction.go_term_frequency,
        )
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .where(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .where(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        stmt = stmt.where(GOPrediction.distance <= ctx.max_distance)
    return stmt


def _count_base_rows(session: Session, ctx: WritePredictionsContext) -> int:
    """Fresh ``COUNT(*)`` over the base filter (cheap cache-invalidation probe)."""
    stmt = (
        select(func.count())
        .select_from(GOPrediction)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .where(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .where(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        stmt = stmt.where(GOPrediction.distance <= ctx.max_distance)
    return int(session.execute(stmt).scalar_one())


def _build_base_frame(session: Session, ctx: WritePredictionsContext) -> tuple[Any, int]:
    """Fetch + dedup the base frame; return ``(deduped_df, raw_row_count)``.

    Dedup keeps the lowest-distance row per ``(protein, go_id)`` (a stable
    sort by ``protein_accession, go_id, distance`` then first), matching the
    ORM path's ``order_by(... distance).first()`` winner and leaving the
    output ordered by ``(protein, go_id)``.
    """
    import pandas as pd

    rows = session.execute(_base_select(ctx)).all()
    df = pd.DataFrame.from_records([tuple(r) for r in rows], columns=list(_BASE_SCORE_COLS))
    raw_count = int(len(df))
    if raw_count:
        df = (
            df.sort_values(["protein_accession", "go_id", "distance"], kind="mergesort")
            .drop_duplicates(subset=["protein_accession", "go_id"], keep="first")
            .reset_index(drop=True)
        )
    return df, raw_count


def _evidence_weight_array(codes: Any, overrides: dict[str, float] | None) -> np.ndarray:
    """Vectorise :func:`evidence_weight` over the (object) evidence-code column."""
    raw = list(codes)
    uniq = {c if isinstance(c, str) else None for c in raw}
    wmap = {c: evidence_weight(c, overrides=overrides) for c in uniq}
    return np.array([wmap[c if isinstance(c, str) else None] for c in raw], dtype=np.float64)


def _float_col(df: Any, name: str) -> np.ndarray:
    """Column ``name`` as a float64 array (``None`` → ``nan``); all-``nan`` if absent."""
    if name in df.columns:
        return df[name].to_numpy(dtype=np.float64)
    return np.full(len(df), np.nan, dtype=np.float64)


def _coverage_array(df: Any) -> np.ndarray:
    """Vectorised mirror of :func:`protea.core.scoring.coverage_signal` (axis A3)."""
    lq = _float_col(df, "length_query")
    aln_sw = _float_col(df, "alignment_length_sw")
    gap_sw = _float_col(df, "gaps_pct_sw")
    aln_nw = _float_col(df, "alignment_length_nw")
    gap_nw = _float_col(df, "gaps_pct_nw")
    use_sw = ~np.isnan(aln_sw)
    aln = np.where(use_sw, aln_sw, aln_nw)
    gap = np.where(use_sw, gap_sw, gap_nw)
    gap_frac = np.where(np.isnan(gap), 0.0, np.clip(gap, 0.0, 1.0))
    valid = (lq > 0.0) & ~np.isnan(aln)
    with np.errstate(invalid="ignore", divide="ignore"):
        cov = np.clip(aln * (1.0 - gap_frac) / lq, 0.0, 1.0)
    return np.where(valid, cov, np.nan)


def _ref_density_array(df: Any) -> np.ndarray:
    """Vectorised mirror of :func:`protea.core.scoring.ref_density_signal` (axis F)."""
    dens = _float_col(df, "ref_annotation_density")
    return np.where(np.isnan(dens), np.nan, 1.0 / (1.0 + np.maximum(0.0, dens)))


def _anc2vec_array(df: Any, name: str) -> np.ndarray:
    """Vectorised mirror of :func:`protea.core.scoring.anc2vec_unit_signal` (axis G)."""
    x = _float_col(df, name)
    return np.where(np.isnan(x), np.nan, (x + 1.0) / 2.0)


def _ia_prior_array(df: Any, params: dict[str, Any]) -> np.ndarray | None:
    """Vectorised mirror of :func:`protea.core.scoring.ia_prior_multiplier` (axis E).

    Returns ``None`` when the prior is disabled (caller skips the multiply),
    otherwise a per-row multiplier array.
    """
    block = (params or {}).get("ia_prior") or {}
    if not block.get("enabled"):
        return None
    gamma = float(block.get("gamma", 1.0))
    source = block.get("source", "frequency")
    if source == IA_PRIOR_SOURCE_IA:
        ia = _float_col(df, "term_ia")
        prior = np.where(np.isnan(ia), 1.0, np.clip(ia, 0.0, 1.0))
    else:
        freq = _float_col(df, "go_term_frequency")
        prior = np.where(np.isnan(freq), 1.0, 1.0 / (1.0 + np.log1p(np.maximum(0.0, freq))))
    prior = np.where(prior <= 0.0, 0.0, prior)
    return np.power(prior, gamma)


def load_ia_map(ia_path: str) -> dict[str, float]:
    """Parse a cafaeval IA TSV (``go_id<TAB>raw_ia``) into ``{go_id: raw_ia}``.

    The IA file is the SAME ``go_id``-keyed information-accretion table the
    cafaeval ``f_micro_w`` metric weights with, so attaching it as the scorer's
    ``ia_prior`` source aligns the prior exactly with the objective. Raw IA is
    unbounded (0 .. ~19); :func:`attach_term_ia` normalises it to [0, 1] before
    the scorer consumes it as ``term_ia``.
    """
    ia_map: dict[str, float] = {}
    with open(ia_path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            go_id = parts[0].strip()
            if not go_id or go_id.lower() in ("go_id", "term"):
                continue
            try:
                ia_map[go_id] = float(parts[1])
            except ValueError:
                continue
    return ia_map


def attach_term_ia(df: Any, ia_map: dict[str, float]) -> None:
    """Inject a normalised ``term_ia`` column (in-place) keyed by ``go_id``.

    Maps each row's ``go_id`` to its raw IA, then min-max-normalises against the
    99th percentile of the positive IA values (clipped to [0, 1]). The p99 anchor
    (rather than the raw max) keeps the common dynamic range from being squashed
    by a handful of extreme-IA terms, while preserving the monotone ordering the
    specificity prior relies on. Terms absent from the IA file get ``NaN`` so the
    scorer leaves them at multiplier 1.0 (no penalty), matching the cafaeval
    uniform-IC fallback. Always overwrites any pre-existing ``term_ia`` column.
    """
    if not ia_map:
        df["term_ia"] = np.nan
        return
    positive = np.array([v for v in ia_map.values() if v > 0.0], dtype=np.float64)
    anchor = float(np.percentile(positive, 99)) if positive.size else 1.0
    if anchor <= 0.0:
        anchor = 1.0
    go_ids = df["go_id"].tolist()
    out = np.empty(len(go_ids), dtype=np.float64)
    for i, gid in enumerate(go_ids):
        raw = ia_map.get(gid)
        out[i] = np.nan if raw is None else min(1.0, max(0.0, raw / anchor))
    df["term_ia"] = out


def _vectorized_scores(df: Any, scoring_config: ScoringConfig | None) -> np.ndarray:
    """Exact, vectorised equivalent of per-row :func:`_score_unranked_pred`.

    Returns the *unrounded* base score array; callers apply ``round(_, 6)``
    only for the ``ScoringConfig`` path (the ``None`` fallback is unrounded,
    matching ``compute_score`` / the distance fallback respectively).
    """
    distance = df["distance"].to_numpy(dtype=np.float64)
    if scoring_config is None:
        return np.maximum(0.0, 1.0 - np.where(np.isnan(distance), 0.0, distance) / 2.0)

    params = getattr(scoring_config, "params", None)
    if not isinstance(params, dict):
        params = {}
    weights = scoring_config.weights
    ev_overrides = scoring_config.evidence_weights or None
    ev_w = _evidence_weight_array(df["evidence_code"].tolist(), ev_overrides)
    tax = df["taxonomic_distance"].to_numpy(dtype=np.float64)
    signals: tuple[tuple[str, np.ndarray], ...] = (
        ("embedding_similarity", np.where(np.isnan(distance), np.nan, 1.0 - distance / 2.0)),
        ("identity_nw", df["identity_nw"].to_numpy(dtype=np.float64)),
        ("identity_sw", df["identity_sw"].to_numpy(dtype=np.float64)),
        ("evidence_weight", ev_w),
        ("taxonomic_proximity", np.where(np.isnan(tax), np.nan, 1.0 / (1.0 + tax))),
        ("neighbor_vote_fraction", df["neighbor_vote_fraction"].to_numpy(dtype=np.float64)),
        ("coverage", _coverage_array(df)),
        ("ref_annotation_density", _ref_density_array(df)),
        ("anc2vec_neighbor_cos", _anc2vec_array(df, "anc2vec_neighbor_cos")),
        ("anc2vec_neighbor_maxcos", _anc2vec_array(df, "anc2vec_neighbor_maxcos")),
    )
    total_w = np.zeros(len(df), dtype=np.float64)
    weighted_sum = np.zeros(len(df), dtype=np.float64)
    for key, values in signals:
        w = float(weights.get(key, 0.0))
        if w == 0.0:
            continue
        valid = ~np.isnan(values)
        weighted_sum += np.where(valid, w * np.clip(values, 0.0, 1.0), 0.0)
        total_w += np.where(valid, w, 0.0)
    base = np.divide(weighted_sum, total_w, out=np.zeros(len(df)), where=total_w > 0.0)
    if scoring_config.formula == FORMULA_EVIDENCE_WEIGHTED:
        base = base * ev_w
    prior = _ia_prior_array(df, params)
    if prior is not None:
        base = base * prior
    # Calibration (params["calibration"]) is a no-op stub today; the scalar
    # path's apply_calibration() is also a pass-through, so the two stay equal.
    return base


def _write_scored_base(df: Any, scoring_config: ScoringConfig | None, path: str) -> None:
    """Score the deduped base frame and write the 4dp ``protein\\tgo_id\\tscore`` TSV."""
    if df.empty:
        with open(path, "w"):
            pass
        return
    scores = _vectorized_scores(df, scoring_config)
    round6 = scoring_config is not None
    proteins = df["protein_accession"].tolist()
    go_ids = df["go_id"].tolist()
    with open(path, "w") as f:
        for i, (protein, go_id) in enumerate(zip(proteins, go_ids, strict=True)):
            score = round(float(scores[i]), 6) if round6 else float(scores[i])
            f.write(f"{protein}\t{go_id}\t{score:.4f}\n")


def write_predictions_reranked(
    session: Session,
    ctx: WritePredictionsContext,
    *,
    reranker_bundle: dict[str, Any],
    known_gos: dict[str, set[str]] | None = None,
) -> None:
    """Write CAFA-format predictions using LightGBM re-ranker scores.

    ``reranker_bundle`` is ``{"model": str, "cat_codes": dict|None,
    "universal": dict|None}``. A non-``None`` ``universal`` block routes
    scoring through :func:`_score_bundle_df` /
    :func:`protea.core._universal_reranker.score_universal` (the universal
    booster carries injected ``plm_id`` / ``k_context`` feature columns the
    generic ``reranker_predict`` cannot encode); the per-cell path is
    unchanged (F-RERANK-UNIVERSAL eval wiring).
    """
    import pandas as pd

    q = (
        session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .filter(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        q = q.filter(GOPrediction.distance <= ctx.max_distance)

    records: list[dict[str, Any]] = [
        _record_from_pred(pred, go_id, aspect=aspect) for pred, go_id, aspect in q.yield_per(5000)
    ]

    if not records:
        with open(ctx.path, "w") as f:
            pass
        return

    df = pd.DataFrame(records)
    if known_gos:
        _patch_query_known_features(df, known_gos)
    df["score"] = _score_bundle_df(df, reranker_bundle)
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
    """Mutate ``df['score']`` by aspect-keyed LightGBM model; fall back to 1 - d/2.

    A bundle carrying a non-``None`` ``universal`` block is scored through
    :func:`_score_bundle_df` / ``score_universal`` (the universal booster's
    injected ``plm_id`` / ``k_context`` columns are unencodable by the generic
    ``reranker_predict``); per-cell bundles keep the generic path unchanged
    (F-RERANK-UNIVERSAL eval wiring).
    """
    df["score"] = 0.0
    for aspect_char, bundle in aspect_models.items():
        mask = df["aspect"] == aspect_char
        if not mask.any():
            continue
        df.loc[mask, "score"] = _score_bundle_df(df.loc[mask], bundle)
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
    Fmax) and the micro-averaged Fmax in ``dfs_best["f_micro"]`` /
    ``dfs_best["f_micro_w"]``; these are surfaced here as the
    ``_w`` / ``_micro`` / ``_micro_w`` keys so chapter-6 tables can
    pull them without re-reading the per-tier TSV artifacts.
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

    _merge_weighted_metrics(dfs_best, ns_results)
    return ns_results


# Maps each extra cafaeval best-frame to the ``cafaeval column -> persisted
# field`` it contributes. The IA-weighted micro frame (the LAFA headline
# metric) also carries the weighted micro precision / recall / coverage;
# persisting those per aspect (not only the unweighted pr/rc) is what makes
# the blob LAFA-comparable. See docs/EVAL_LAFA_PARITY.md and FIX-METRIC-IA.
_EXTRA_METRIC_SPECS: tuple[tuple[str, dict[str, str]], ...] = (
    ("f_w", {"f_w": "fmax_w"}),
    ("f_micro", {"f_micro": "f_micro"}),
    (
        "f_micro_w",
        {
            "f_micro_w": "f_micro_w",
            "pr_micro_w": "precision_w",
            "rc_micro_w": "recall_w",
            "cov_max": "coverage_w",
        },
    ),
)


def _merge_weighted_metrics(dfs_best: dict, ns_results: dict[str, Any]) -> None:
    """Fold the IA-weighted / micro best-frames into ``ns_results`` in place.

    Each frame is keyed by namespace; only namespaces already present from the
    unweighted ``f`` frame are touched. Missing frames (uniform IC=1 fallback)
    are skipped, leaving the unweighted keys alone.

    The namespace key must be unique per frame. If it were not, the last row
    for a namespace would overwrite the earlier ones and the published metric
    would silently belong to a different row than the one it is attributed to,
    with the result keeping its expected shape throughout. That is checked
    rather than assumed, because it is the failure mode that produces no error.
    """
    for key, col_to_field in _EXTRA_METRIC_SPECS:
        df_extra = dfs_best.get(key)
        if df_extra is None or df_extra.empty:
            continue
        df_extra = df_extra.reset_index()
        assert_unique_key(
            (str(r.get("ns", "")) for _, r in df_extra.iterrows()),
            lambda ns: ns,
            context=f"folding the {key!r} best-frame into the per-namespace results",
        )
        for _, row in df_extra.iterrows():
            ns = _NS_LABELS.get(str(row.get("ns", "")))
            if ns is None or ns not in ns_results:
                continue
            for col, field in col_to_field.items():
                if col in row:
                    ns_results[ns][field] = round(float(row.get(col, 0)), 4)
