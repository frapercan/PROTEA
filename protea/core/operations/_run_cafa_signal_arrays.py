"""Vectorised mirrors of the scalar signal functions in :mod:`protea.core.scoring`.

``_run_cafa_artifacts`` scores the base frame twice over: once per row through
:func:`protea.core.scoring.compute_score`, for the paths that hold ORM
objects, and once as arrays, because a grid cell is 1.2M rows and seven
configs. The array forms have to be exact equivalents of the scalar ones,
which is a property of each pair and the reason they are mirrors rather than
two implementations: every function here names the scalar function it
reproduces.

They live in their own module because the writer they serve crossed the
master-plan v3.2 section 3 file budget, and because this is the one group in
it that answers a single question -- how a column becomes a signal array --
and is used nowhere else.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from protea.core.scoring import MethodScoreMissing, evidence_weight
from protea.infrastructure.orm.models.embedding.scoring_config import IA_PRIOR_SOURCE_IA


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


def _method_score_array(df: Any) -> np.ndarray:
    """The method's own scores, vectorised, refusing a frame without them.

    The scalar path raises per row through
    :func:`protea.core.scoring.method_score_of`; this one checks the column
    once and then the whole array, because a method prediction set either
    carries the column throughout or was not written by a method at all.

    A base frame cached before the column existed does not have it, and the
    cache key does not name the schema, so that case is reported as what it
    is rather than read as all-missing.
    """
    if "method_score" not in getattr(df, "columns", ()):
        raise MethodScoreMissing(
            "the base frame carries no method_score column; it was cached "
            "before the column existed, so the memoised parquet for this "
            "prediction set has to be rebuilt"
        )
    values = df["method_score"].to_numpy(dtype=np.float64)
    missing = int(np.count_nonzero(np.isnan(values)))
    if missing:
        raise MethodScoreMissing(
            f"{missing} of {len(values)} rows carry no method_score while the "
            "scoring config asks for the method's own score"
        )
    return values
