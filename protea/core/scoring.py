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

import math
from collections import defaultdict
from typing import Any

from protea.core.evidence_codes import ECO_TO_CODE
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHT_FALLBACK,
    DEFAULT_EVIDENCE_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    IA_PRIOR_SOURCE_IA,
    ScoringConfig,
)

__all__ = [
    "DEFAULT_EVIDENCE_WEIGHT_FALLBACK",
    "DEFAULT_EVIDENCE_WEIGHTS",
    "FORMULA_EVIDENCE_WEIGHTED",
    "evidence_weight",
    "compute_score",
    "coverage_signal",
    "ref_density_signal",
    "anc2vec_unit_signal",
    "ia_prior_multiplier",
    "apply_calibration",
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


def coverage_signal(pred: dict[str, Any]) -> float | None:
    """Single principled alignment-coverage signal in [0, 1] (axis A3).

    Coverage answers *how much of the query is spanned by the aligned region*.
    We prefer the Smith-Waterman **local** alignment: the Needleman-Wunsch
    **global** ``alignment_length`` is always ~``max(len_query, len_ref)`` so
    its coverage saturates near 1 and is uninformative; the local span captures
    the domain-level match extent that actually discriminates transfers.

    Definition (``alignment_length`` is the aligned column count, ``gaps_pct``
    is the gap *fraction* in [0, 1], not a percentage)::

        ungapped_aligned = alignment_length_sw * (1 - gaps_pct_sw)
        coverage = clip(ungapped_aligned / length_query, 0, 1)

    ``ungapped_aligned`` is the number of non-gap aligned columns, i.e. the
    count of query residues inside the alignment core; dividing by the query
    length gives the covered fraction. Falls back to the NW fields when the SW
    fields are absent, and drops out (``None``) when ``length_query`` is
    missing / non-positive or no alignment length is available.
    """
    length_query = pred.get("length_query")
    if length_query is None or float(length_query) <= 0.0:
        return None
    aln = pred.get("alignment_length_sw")
    gap = pred.get("gaps_pct_sw")
    if aln is None:
        aln = pred.get("alignment_length_nw")
        gap = pred.get("gaps_pct_nw")
    if aln is None:
        return None
    gap_frac = 0.0 if gap is None else max(0.0, min(1.0, float(gap)))
    ungapped = float(aln) * (1.0 - gap_frac)
    return max(0.0, min(1.0, ungapped / float(length_query)))


def ref_density_signal(pred: dict[str, Any]) -> float | None:
    """Reference-annotation-density signal in (0, 1] (axis F).

    ``ref_annotation_density`` is the number of GO annotations carried by the
    reference (neighbour) protein. A reference with *fewer* annotations is more
    specific, so a term transferred from it is a more pointed claim. We squash
    the raw count with ``1 / (1 + density)``: density 0 → 1.0, large density →
    →0, monotonically decreasing. Drops out (``None``) when the column is
    absent.
    """
    density = pred.get("ref_annotation_density")
    if density is None:
        return None
    return 1.0 / (1.0 + max(0.0, float(density)))


def anc2vec_unit_signal(value: float | None) -> float | None:
    """Map an anc2vec cosine in [-1, 1] onto [0, 1] (axis G).

    ``anc2vec_neighbor_cos`` / ``anc2vec_neighbor_maxcos`` are cosine
    similarities of the candidate term against the neighbour's annotation
    centroid, so they live in [-1, 1]. The affine map ``(x + 1) / 2`` preserves
    ordering while landing in the [0, 1] range the weighted average expects.
    Drops out (``None``) when the column is absent.
    """
    if value is None:
        return None
    return (float(value) + 1.0) / 2.0


def ia_prior_multiplier(pred: dict[str, Any], params: dict[str, Any] | None) -> float:
    """Multiplicative term-specificity prior in [0, 1] (axis E).

    The IA-weighted ``f_micro_w`` objective rewards specific (high-IA) terms,
    yet the base composite ignores term specificity entirely. This prior closes
    that gap: the composite score is multiplied by ``prior_t ** gamma`` where
    ``prior_t in (0, 1]`` is a specificity weight and ``gamma >= 0`` tunes its
    strength (``gamma = 0`` → multiplier 1 → no effect, a second off-switch).

    Gated by ``params["ia_prior"]["enabled"]`` (default off → returns ``1.0``,
    preserving the legacy score exactly). Two sources:

    - ``"frequency"`` (default): from the term's corpus count
      ``go_term_frequency``. ``prior = 1 / (1 + log1p(freq))`` — common terms
      (large freq) are down-weighted, rare/specific terms keep ~1.
    - ``"ia"``: from a normalised information-accretion value in [0, 1] supplied
      on the pred as ``term_ia`` (the caller resolves it from the
      config-referenced IA file / ``OntologySnapshot``; A-SCORE.2 plug point).
      ``prior = clip(term_ia, 0, 1)``.

    When the source field is missing on the pred the prior is ``1.0`` (no
    penalty) rather than 0, so a partially-featured row is never silently
    zeroed.
    """
    block = (params or {}).get("ia_prior") or {}
    if not block.get("enabled"):
        return 1.0
    gamma = float(block.get("gamma", 1.0))
    source = block.get("source", "frequency")
    if source == IA_PRIOR_SOURCE_IA:
        ia = pred.get("term_ia")
        prior = 1.0 if ia is None else max(0.0, min(1.0, float(ia)))
    else:
        freq = pred.get("go_term_frequency")
        prior = 1.0 if freq is None else 1.0 / (1.0 + math.log1p(max(0.0, float(freq))))
    if prior <= 0.0:
        return 0.0
    return float(prior**gamma)


def apply_calibration(
    score: float,
    *,
    aspect: str | None = None,
    ia_bin: int | None = None,
    calibration: dict[str, Any] | None = None,
) -> float:
    """Post-score per-(aspect, IA-bin) calibration hook (axis: calibration).

    No-op stub with a fixed interface so A-SCORE.2 can drop in isotonic
    mappings without touching any caller. Returns ``score`` unchanged unless
    ``calibration["enabled"]`` is set *and* a populated table is supplied
    (none ships today). Gated off by default → byte-identical to the legacy
    score.
    """
    block = calibration or {}
    if not block.get("enabled"):
        return score
    # Future (A-SCORE.2): look up an isotonic mapping keyed by (aspect, ia_bin)
    # in block["tables"] and apply it. Until those tables exist this is a
    # transparent pass-through.
    return score


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
    - ``coverage`` (axis A3): see :func:`coverage_signal`.
    - ``ref_annotation_density`` (axis F): see :func:`ref_density_signal`.
    - ``anc2vec_neighbor_cos`` / ``anc2vec_neighbor_maxcos`` (axis G):
      see :func:`anc2vec_unit_signal`.
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
        ("coverage", coverage_signal(pred)),
        ("ref_annotation_density", ref_density_signal(pred)),
        ("anc2vec_neighbor_cos", anc2vec_unit_signal(pred.get("anc2vec_neighbor_cos"))),
        ("anc2vec_neighbor_maxcos", anc2vec_unit_signal(pred.get("anc2vec_neighbor_maxcos"))),
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

    Composition order (each step is a no-op when its config knob is off, so a
    config without ``params`` scores exactly as before these knobs existed):
    weighted average, then ``x evidence_weight`` for the evidence_weighted
    formula, then ``x ia_prior_multiplier`` (axis E), then ``apply_calibration``.
    """
    params = getattr(config, "params", None)
    if not isinstance(params, dict):
        params = {}
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
    base_score *= ia_prior_multiplier(pred, params)
    base_score = apply_calibration(
        base_score,
        aspect=pred.get("aspect"),
        ia_bin=pred.get("ia_bin"),
        calibration=params.get("calibration"),
    )
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
