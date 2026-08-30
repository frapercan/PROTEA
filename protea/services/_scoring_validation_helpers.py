"""Validation + signal-coverage helpers extracted from ``scoring_service``.

These pure functions own the heavy DB introspection (per-signal NULL
counts, temporal NK/LK/PK delta materialisation) for the scoring API.
They return raw results; the service-layer wrappers translate them
into domain exceptions (``SignalCoverageError``, ``EntityNotFoundError``)
so the helper module stays free of those types.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data_for_sets
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_EVIDENCE_WEIGHTED,
    ScoringConfig,
)

#: Which stored column backs each scoring signal. This map is what the
#: coverage check can see, and a weighted signal absent from it used to be
#: skipped: the loop walked the MAP and asked about each of its entries, so a
#: signal nobody had mapped was never asked about at all. The check then
#: returned "nothing missing" and the scoring ran, weighting a column that is
#: NULL on every row, which contributes 0.0 to every score in silence.
#:
#: It is not hypothetical. anc2vec_neighbor_cos and anc2vec_neighbor_maxcos
#: are real GOPrediction columns, non-null in 0 of 2,441,584 rows of the
#: current set, and neither is here. No scoring config weights them today (0
#: of 8), so nothing has been scored wrongly yet, and the next one to try
#: would have been approved.
_SIGNAL_TO_COLUMN: dict[str, str] = {
    "embedding_similarity": "distance",
    "identity_nw": "identity_nw",
    "identity_sw": "identity_sw",
    "evidence_weight": "evidence_code",
    "taxonomic_proximity": "taxonomic_distance",
    "neighbor_vote_fraction": "neighbor_vote_fraction",
}


def compute_missing_signals(
    session: Session,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
) -> list[str]:
    """Return human-readable strings for every required signal absent from the set.

    For each signal with a non-zero weight in ``config_snap.weights``
    (plus ``evidence_code`` when the formula is ``evidence_weighted``,
    where the multiplier is always applied), count how many rows in the
    PredictionSet have the backing column non-NULL. Empty list means
    full coverage. The service-layer wrapper raises
    ``SignalCoverageError`` when this list is non-empty.
    """
    weights = config_snap.weights or {}
    required: list[tuple[str, str]] = []
    unmappable: list[str] = []
    # Walk the WEIGHTS, not the map. Walking the map asks only about signals
    # someone remembered to map, so an unmapped one passes by never being
    # asked, which is the opposite of a check.
    for signal, weight in weights.items():
        if float(weight or 0.0) <= 0.0:
            continue
        col = _SIGNAL_TO_COLUMN.get(signal)
        if col is None:
            unmappable.append(signal)
        else:
            required.append((signal, col))
    if getattr(config_snap, "formula", "linear") == FORMULA_EVIDENCE_WEIGHTED and not any(
        s == "evidence_weight" for s, _ in required
    ):
        required.append(("evidence_weight", "evidence_code"))
    if not required:
        return list(_unmappable_messages(unmappable))

    cols_sql = ", ".join(f"COUNT({col}) AS cnt_{col}" for _, col in required)
    row = (
        session.execute(
            text(
                f"SELECT COUNT(*) AS total, {cols_sql} "  # noqa: S608 — col names hard-coded
                "FROM go_prediction WHERE prediction_set_id = :pid"
            ),
            {"pid": str(prediction_set_id)},
        )
        .mappings()
        .one()
    )
    total = int(row["total"] or 0)
    missing: list[str] = list(_unmappable_messages(unmappable))
    for signal, col in required:
        cnt = int(row[f"cnt_{col}"] or 0)
        if total == 0 or cnt == 0:
            missing.append(f"{signal} (column '{col}': {cnt}/{total} rows)")
    return missing


def build_training_gt_pairs(
    session: Session,
    *,
    prediction_set: PredictionSet,
    evaluation_set: Any,
    category: str,
) -> set[tuple[str, str]]:
    """Compute the ``(protein, go_id)`` ground-truth pair set for training.

    The caller (service layer) is responsible for resolving the
    ``PredictionSet`` and ``EvaluationSet`` rows and raising
    ``EntityNotFoundError`` for missing ids. ``evaluation_set`` is
    typed as ``Any`` so the helper can stay free of the
    ``EvaluationSet`` ORM import (which lives at module level via
    lazy import in the service).
    """
    # The prediction set's snapshot is the PIVOT here, the universe to answer
    # in. It says nothing about where the two annotation sets live, and using
    # it to resolve them silently drops everything that is not native to it.
    eval_data = compute_evaluation_data_for_sets(
        session,
        old_annotation_set_id=evaluation_set.old_annotation_set_id,
        new_annotation_set_id=evaluation_set.new_annotation_set_id,
        pivot_snapshot_id=prediction_set.ontology_snapshot_id,
    )
    ground_truth: dict[str, set[str]] = getattr(eval_data, category)
    gt_pairs: set[tuple[str, str]] = set()
    for protein, go_ids in ground_truth.items():
        for go_id in go_ids:
            gt_pairs.add((protein, go_id))
    return gt_pairs


def _unmappable_messages(signals: list[str]) -> list[str]:
    """A weighted signal with no known column is reported, not skipped.

    Refusing here is the conservative direction and the cheap one: the fix is
    to add the signal to ``_SIGNAL_TO_COLUMN``, which is one line, and the
    alternative is a score that silently weights nothing. It is reported in the
    same list as a missing column because to a caller the two are the same
    problem, a signal that will not contribute what the config says it should.
    """
    return [
        f"{signal} (no column is mapped for this signal, so its coverage "
        "cannot be checked; add it to _SIGNAL_TO_COLUMN)"
        for signal in sorted(signals)
    ]
