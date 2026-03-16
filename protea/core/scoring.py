"""Scoring engine for GOPrediction rows.

Applies a :class:`~protea.infrastructure.orm.models.embedding.scoring_config.ScoringConfig`
formula to raw prediction signals and returns a normalised [0, 1] confidence score.

The engine is intentionally *stateless*: every call to :func:`compute_score`
is self-contained, which means any ``ScoringConfig`` can be applied to an
existing ``PredictionSet`` at any time without re-running the KNN search.

Evidence-code weights
---------------------
Evidence code quality is resolved through a two-level lookup:

1. If ``config.evidence_weights`` is not ``None``, that dict is checked first.
2. For codes absent from the override (or when no override exists), the module-
   level :data:`DEFAULT_EVIDENCE_WEIGHTS` table is used.
3. Codes unknown to both tables fall back to
   :data:`DEFAULT_EVIDENCE_WEIGHT_FALLBACK` (0.5).

This means a ``ScoringConfig`` may carry a *partial* override — e.g. only
changing the IEA weight from 0.3 to 0.0 — without having to redeclare every
other code.  The resolution order ensures backwards compatibility: configs
stored without ``evidence_weights`` behave identically to older configs.
"""

from __future__ import annotations

from typing import Any

from protea.core.evidence_codes import ECO_TO_CODE
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHT_FALLBACK,
    DEFAULT_EVIDENCE_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    ScoringConfig,
)

# ---------------------------------------------------------------------------
# Evidence-code weight resolution
# ---------------------------------------------------------------------------


def evidence_weight(
    code: str | None,
    *,
    overrides: dict[str, float] | None = None,
) -> float:
    """Resolve the [0, 1] quality weight for a GO evidence code or ECO ID.

    Resolution order
    ----------------
    1. Normalise *code* from ECO ID to GO code via :data:`ECO_TO_CODE` if needed.
    2. Look up the normalised code in *overrides* (if provided).
    3. Fall back to :data:`DEFAULT_EVIDENCE_WEIGHTS`.
    4. If still not found, return :data:`DEFAULT_EVIDENCE_WEIGHT_FALLBACK`.

    Parameters
    ----------
    code:
        A GO evidence code (e.g. ``"IEA"``) or an ECO URI
        (e.g. ``"ECO:0000501"``).  ``None`` returns the fallback weight.
    overrides:
        Optional per-config evidence weight table.  May be a partial dict;
        codes not present here are resolved via :data:`DEFAULT_EVIDENCE_WEIGHTS`.

    Returns
    -------
    float in [0, 1].
    """
    if not code:
        return DEFAULT_EVIDENCE_WEIGHT_FALLBACK

    # Normalise ECO IDs to canonical GO evidence codes.
    normalized = ECO_TO_CODE.get(code, code)

    # Config-level override takes precedence over the system default.
    if overrides and normalized in overrides:
        return float(overrides[normalized])

    return DEFAULT_EVIDENCE_WEIGHTS.get(normalized, DEFAULT_EVIDENCE_WEIGHT_FALLBACK)


# ---------------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------------


def compute_score(pred: dict[str, Any], config: ScoringConfig) -> float:
    """Compute a [0, 1] confidence score for a single GOPrediction dict.

    All signals are normalised to [0, 1] before weighting.  Signals whose
    value is ``None`` (because the corresponding feature-engineering flag was
    not enabled at prediction time) are *silently excluded* from both the
    numerator and the denominator, so the remaining signals still produce a
    valid normalised score.

    Parameters
    ----------
    pred:
        Dict with raw prediction fields.  Recognised keys:

        - ``distance`` (float): cosine distance in [0, 2].
        - ``identity_nw`` (float | None): NW global identity in [0, 1].
        - ``identity_sw`` (float | None): SW local identity in [0, 1].
        - ``evidence_code`` (str | None): GO or ECO evidence code.
        - ``taxonomic_distance`` (float | None): raw taxonomic distance.

    config:
        A :class:`ScoringConfig` instance defining the formula, signal
        weights, and optional per-code evidence weight overrides.

    Returns
    -------
    float in [0, 1].  Higher values indicate higher predicted confidence.
    The result is rounded to 6 decimal places.
    """
    signal_weights = config.weights
    ev_overrides: dict[str, float] | None = config.evidence_weights or None

    total_w = 0.0
    weighted_sum = 0.0

    def _add(key: str, value: float | None) -> None:
        """Add one signal's contribution to the running weighted average."""
        nonlocal total_w, weighted_sum
        w = float(signal_weights.get(key, 0.0))
        if w == 0.0 or value is None:
            return
        total_w += w
        weighted_sum += w * max(0.0, min(1.0, value))

    # 1. Embedding similarity: cosine distance [0, 2] → similarity [0, 1].
    distance = pred.get("distance")
    if distance is not None:
        _add("embedding_similarity", 1.0 - distance / 2.0)

    # 2. Global sequence identity (Needleman-Wunsch).
    _add("identity_nw", pred.get("identity_nw"))

    # 3. Local sequence identity (Smith-Waterman).
    _add("identity_sw", pred.get("identity_sw"))

    # 4. Evidence code quality — resolved with per-config overrides.
    ev_w = evidence_weight(pred.get("evidence_code"), overrides=ev_overrides)
    _add("evidence_weight", ev_w)

    # 5. Taxonomic proximity: 1 / (1 + d) maps [0, ∞) → (0, 1].
    tax_dist = pred.get("taxonomic_distance")
    if tax_dist is not None:
        _add("taxonomic_proximity", 1.0 / (1.0 + float(tax_dist)))

    if total_w == 0.0:
        return 0.0

    base_score = weighted_sum / total_w

    # evidence_weighted formula: multiply the final score by the resolved
    # evidence quality so that low-confidence annotations (IEA, ND) are
    # down-ranked even when other signals are strong — and regardless of
    # whether the evidence_weight signal is active (its signal weight may be 0).
    if config.formula == FORMULA_EVIDENCE_WEIGHTED:
        base_score *= ev_w

    return round(base_score, 6)


# ---------------------------------------------------------------------------
# Batch helper
# ---------------------------------------------------------------------------


def score_predictions(
    predictions: list[dict[str, Any]],
    config: ScoringConfig,
) -> list[dict[str, Any]]:
    """Add a ``score`` key to each prediction dict and return them sorted descending.

    Parameters
    ----------
    predictions:
        List of raw prediction dicts (same format as accepted by
        :func:`compute_score`).
    config:
        The :class:`ScoringConfig` to apply.

    Returns
    -------
    A new list with a ``score`` key added to each item, sorted by score in
    descending order.  The original list is not modified.
    """
    scored = [{**p, "score": compute_score(p, config)} for p in predictions]
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored
