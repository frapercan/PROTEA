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

This means a ``ScoringConfig`` may carry a *partial* override — e.g. zeroing
the IEA weight for an experiment-only study — without having to redeclare
every other code.  The resolution order ensures backwards compatibility:
configs stored without ``evidence_weights`` behave identically to older
configs.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from protea.core.evidence_codes import ECO_TO_CODE
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHT_FALLBACK,
    DEFAULT_EVIDENCE_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    ScoringConfig,
)

__all__ = [
    "DEFAULT_EVIDENCE_WEIGHT_FALLBACK",
    "DEFAULT_EVIDENCE_WEIGHTS",
    "FORMULA_EVIDENCE_WEIGHTED",
    "evidence_weight",
    "compute_score",
    "score_predictions",
    "propagate_scores_to_ancestors",
    "propagate_ground_truth_to_ancestors",
]

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


def _resolve_signal_values(
    pred: dict[str, Any],
    evidence_w: float,
) -> list[tuple[str, float | None]]:
    """Map raw prediction fields to ``(signal_key, normalised_value)`` pairs.

    Each entry is fed through ``compute_score``'s weighted-average loop;
    ``None`` values mean the corresponding feature-engineering flag was
    off at prediction time and the signal must drop out of both
    numerator and denominator.

    Recognised pred keys:

    - ``distance`` (float): cosine distance in [0, 2] → similarity 1 - d/2.
    - ``identity_nw`` / ``identity_sw`` (float | None): NW / SW identity in [0, 1].
    - ``taxonomic_distance`` (float | None): mapped via 1 / (1 + d).
    - ``neighbor_vote_fraction`` (float | None): already in [0, 1].
    """
    distance = pred.get("distance")
    embedding_sim = 1.0 - float(distance) / 2.0 if distance is not None else None
    tax_dist = pred.get("taxonomic_distance")
    tax_proximity = 1.0 / (1.0 + float(tax_dist)) if tax_dist is not None else None
    return [
        ("embedding_similarity", embedding_sim),
        ("identity_nw", pred.get("identity_nw")),
        ("identity_sw", pred.get("identity_sw")),
        ("evidence_weight", evidence_w),
        ("taxonomic_proximity", tax_proximity),
        ("neighbor_vote_fraction", pred.get("neighbor_vote_fraction")),
    ]


def compute_score(pred: dict[str, Any], config: ScoringConfig) -> float:
    """Compute a [0, 1] confidence score for a single GOPrediction dict.

    Signals are normalised to [0, 1] then weighted-averaged. Signals
    whose value is ``None`` (feature-engineering flag off at predict
    time) drop from both numerator and denominator. Returns
    ``round(base_score, 6)``; ``FORMULA_EVIDENCE_WEIGHTED`` multiplies
    the average by the resolved evidence weight even when the
    evidence_weight signal carries weight 0 — so IEA / ND annotations
    get down-ranked regardless of feature configuration.
    """
    signal_weights = config.weights
    ev_overrides = config.evidence_weights or None
    ev_w = evidence_weight(pred.get("evidence_code"), overrides=ev_overrides)
    total_w = 0.0
    weighted_sum = 0.0
    for key, value in _resolve_signal_values(pred, ev_w):
        if value is None:
            continue
        w = float(signal_weights.get(key, 0.0))
        if w == 0.0:
            continue
        total_w += w
        weighted_sum += w * max(0.0, min(1.0, value))
    if total_w == 0.0:
        return 0.0
    base_score = weighted_sum / total_w
    if config.formula == FORMULA_EVIDENCE_WEIGHTED:
        base_score *= ev_w
    return round(base_score, 6)


# ---------------------------------------------------------------------------
# Batch helper
# ---------------------------------------------------------------------------


def propagate_scores_to_ancestors(
    scored_predictions: list[dict[str, Any]],
    parent_map: dict[str, set[str]],
) -> list[dict[str, Any]]:
    """Max-propagate GO scores to ancestors per protein (True Path Rule).

    For every (protein, go_id, score) row, ensures every ancestor ``a`` of
    ``go_id`` in ``parent_map`` also has score ≥ that row's score for the
    same protein. The final score of an ancestor is the max over all its
    descendants predicted for the protein (including itself if predicted).

    This mirrors cafaeval's behaviour: annotating a child implies every
    ancestor, so predictions must propagate upward before PR metrics.
    ``parent_map`` maps a child ``go_id`` (string) to the set of its direct
    parent ``go_id`` strings — typically the union of ``is_a`` + ``part_of``
    edges for the relevant ontology snapshot.

    Returns a new list; the input is not modified. Rows that share a
    (protein, go_id) collapse to a single row keyed by the max score.
    """
    ancestor_closure: dict[str, set[str]] = {}

    def ancestors_of(go_id: str) -> set[str]:
        cached = ancestor_closure.get(go_id)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [go_id]
        while stack:
            node = stack.pop()
            for parent in parent_map.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        ancestor_closure[go_id] = seen
        return seen

    by_protein: defaultdict[str, dict[str, float]] = defaultdict(dict)
    for p in scored_predictions:
        acc = p["protein_accession"]
        gid = str(p["go_id"])
        s = float(p["score"])
        cur = by_protein[acc]
        if s > cur.get(gid, -1.0):
            cur[gid] = s

    propagated: list[dict[str, Any]] = []
    for acc, term_scores in by_protein.items():
        expanded: dict[str, float] = dict(term_scores)
        for gid, s in term_scores.items():
            for anc in ancestors_of(gid):
                if s > expanded.get(anc, -1.0):
                    expanded[anc] = s
        for gid, s in expanded.items():
            propagated.append({"protein_accession": acc, "go_id": gid, "score": s})

    return propagated


def propagate_ground_truth_to_ancestors(
    ground_truth: dict[str, set[str]],
    parent_map: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Expand each protein's GO set with every ancestor in ``parent_map``.

    Returns a new dict; the input is not modified. Pair with
    :func:`propagate_scores_to_ancestors` when the downstream metric treats
    predictions and ground truth symmetrically (e.g. cafaeval-style Fmax).
    """
    ancestor_closure: dict[str, set[str]] = {}

    def ancestors_of(go_id: str) -> set[str]:
        cached = ancestor_closure.get(go_id)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [go_id]
        while stack:
            node = stack.pop()
            for parent in parent_map.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        ancestor_closure[go_id] = seen
        return seen

    expanded: dict[str, set[str]] = {}
    for acc, gos in ground_truth.items():
        full: set[str] = set(gos)
        for gid in gos:
            full.update(ancestors_of(str(gid)))
        expanded[acc] = full
    return expanded


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
