"""The arithmetic that turns a candidate's evidence into a score.

A transfer method's candidates are stored with their evidence, not with a
score: the distance to the donor, the alignment identities, the share of
neighbours that voted for the term, and so on. A ``ScoringConfig`` names how
that evidence becomes one number: a weighted average of signals in [0, 1],
optionally multiplied by the evidence weight and by an information prior.

This module is that arithmetic and the read of the candidates it needs, and
nothing else. ``score_prediction_set`` applies it when it scores a candidate
set and stores the result as the scored set's ``method_score``, which is what
the evaluator reads.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy import func, select

from protea.core.operations._base_frame_recount import ledger_columns
from protea.core.operations._donor_recount import NEIGHBOURHOOD_SIGNALS
from protea.core.operations._run_cafa_signal_arrays import (
    _anc2vec_array,
    _coverage_array,
    _evidence_weight_array,
    _ia_prior_array,
    _method_score_array,
    _ref_density_array,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_EVIDENCE_WEIGHTED,
    FORMULA_METHOD_SCORE,
    ScoringConfig,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = (
    "BASE_SCORE_COLS",
    "candidate_select",
    "count_candidates",
    "vectorized_scores",
)

#: The columns the scorer reads off each candidate, in the order the SELECT
#: returns them. Fetched as a Core columnar query (never 1.2M ORM objects),
#: deduped to the lowest-distance row per (protein, go_id), then vectorised.
BASE_SCORE_COLS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "distance",
    "identity_nw",
    "identity_sw",
    "evidence_code",
    "taxonomic_distance",
    "neighbor_vote_fraction",
    # Voters, as opposed to vote_count's annotation rows. Selected always,
    # not only under a cut, so the frame's schema does not depend on which
    # arm built it. Under a cut the recount overwrites it with the count at
    # that depth; without one it is what the retrieval wrote.
    "donor_count",
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
    # The method's own score, for the formula that returns it unchanged.
    # Selected always, like donor_count, so the frame's schema does not
    # depend on which formula will read it.
    "method_score",
)


def _column(name: str) -> Any:
    """The SELECT column for one name: the GO id lives on the term, the rest on the row."""
    return GOTerm.go_id if name == "go_id" else getattr(GOPrediction, name)


def _filtered(stmt: Any, ctx: Any) -> Any:
    """The candidates of ``ctx``: its set, its proteins, and its depth.

    One filter for the SELECT and for the COUNT, so the depth guard compares a
    frame against a count of exactly the rows it was built from.
    """
    stmt = (
        stmt.join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .where(GOPrediction.prediction_set_id == ctx.pred_set_id)
        .where(GOPrediction.protein_accession.in_(ctx.delta_proteins))
    )
    if ctx.max_distance is not None:
        stmt = stmt.where(GOPrediction.distance <= ctx.max_distance)
    if ctx.max_k_position is not None:
        stmt = stmt.where(GOPrediction.k_position <= ctx.max_k_position)
    if ctx.max_sequence_rank is not None:
        stmt = stmt.where(GOPrediction.sequence_rank <= ctx.max_sequence_rank)
    return stmt


def candidate_select(ctx: Any, *, with_ledger: bool = False, extra: tuple[str, ...] = ()) -> Any:
    """Core columnar SELECT of :data:`BASE_SCORE_COLS`, then ``extra``, then the ledger.

    ``extra`` names further columns of the row a caller needs beside the
    scorer's, in the order given; the frame built from this SELECT has to list
    them in the same place. The donor ledger comes last, and only under a cut
    (``ledger_columns``).
    """
    columns = [_column(name) for name in (*BASE_SCORE_COLS, *extra)]
    return _filtered(select(*columns, *ledger_columns(with_ledger)), ctx)


def count_candidates(session: Session, ctx: Any) -> int:
    """Fresh ``COUNT(*)`` over the same filter (the depth guard's probe)."""
    stmt = _filtered(select(func.count()).select_from(GOPrediction), ctx)
    return int(session.execute(stmt).scalar_one())


def _neighbourhood_array(df: Any, name: str, weights: dict[str, Any]) -> np.ndarray:
    """A neighbourhood signal's column, or NaN where it was neither weighted nor computed.

    The two neighbourhood signals are not stored: the frame's recount computes
    them at the cut, and only for a configuration that weights them
    (``_base_frame_recount``). A frame without one its configuration weights was
    built without it, and scoring it would hand every row the other signals'
    score in silence, so that is refused.
    """
    if name in df.columns:
        return df[name].to_numpy(dtype=np.float64)
    if float(weights.get(name) or 0.0) != 0.0:
        raise ValueError(
            f"the configuration weights {name}, which is computed at a depth from the donor "
            "ledger, and this frame was built without it"
        )
    return np.full(len(df), np.nan)


def vectorized_scores(df: Any, scoring_config: ScoringConfig) -> np.ndarray:
    """The score of every row of the frame under ``scoring_config``.

    Returns the *unrounded* base score array: the four-decimal formatting of
    the prediction file is the only rounding, applied by whoever writes it.
    There is no default: the evaluator's old fallback for a run that named no
    configuration, ``1 - distance / 2``, is ``embedding_only`` for every
    candidate that carries a distance.
    """
    distance = df["distance"].to_numpy(dtype=np.float64)
    if scoring_config.formula == FORMULA_METHOD_SCORE:
        return _method_score_array(df)

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
        *((name, _neighbourhood_array(df, name, weights)) for name in NEIGHBOURHOOD_SIGNALS),
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
