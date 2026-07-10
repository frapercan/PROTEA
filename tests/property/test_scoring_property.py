"""Property tests for ``protea.core.scoring``.

Invariants exercised:

* ``compute_score`` always returns a value in ``[0, 1]`` regardless of
  signal-weight or evidence-weight overrides.
* ``score_predictions`` returns its list sorted descending by score
  (a non-obvious property because ``compute_score`` uses ``round(.., 6)``,
  which can produce equal scores from distinct inputs and the sort is
  stable but the inputs may not be).
* Out-of-range signals (``identity_nw < 0``, ``identity_nw > 1``)
  clamp instead of crashing or leaking outside [0, 1].
* Evidence-weighted formula is bounded above by the linear formula for
  the same signals: the evidence multiplier is in [0, 1] so it can
  only shrink the score.
* ``propagate_scores_to_ancestors`` is idempotent: applying it twice
  yields the same set of (protein, go, score) tuples.
* ``propagate_scores_to_ancestors`` respects max-monotonicity: every
  ancestor's score is >= the max score of any descendant predicted
  for the same protein.

Strategies generate prediction dicts in the same shape ``compute_score``
accepts: numeric signals in their declared ranges plus an optional
evidence code drawn from the known table.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from protea.core.scoring import (
    compute_score,
    propagate_scores_to_ancestors,
    score_predictions,
)
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_EVIDENCE_WEIGHTS,
    FORMULA_EVIDENCE_WEIGHTED,
    FORMULA_LINEAR,
    ScoringConfig,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Signal weights are non-negative reals; at least one must be > 0 to avoid
# the degenerate total_w == 0 short-circuit. We generate full dicts so the
# resolution path inside ``compute_score`` (``signal_weights.get(key, 0)``)
# does not silently mask a regression.
SIGNAL_KEYS = (
    "embedding_similarity",
    "identity_nw",
    "identity_sw",
    "evidence_weight",
    "taxonomic_proximity",
    "neighbor_vote_fraction",
)

# A weight is either switched off (exactly 0.0) or a magnitude the system can
# actually express. Nothing in between: drawing arbitrarily small positives lets
# Hypothesis reach the subnormal range, where `weight * signal` and `weight *
# scale` underflow to zero. Scale invariance is a property of exact arithmetic,
# not of IEEE 754, so such draws falsify a formula that is in fact correct.
weight_value = st.one_of(
    st.just(0.0),
    st.floats(
        min_value=1e-6,
        max_value=10.0,
        allow_nan=False,
        allow_infinity=False,
        allow_subnormal=False,
    ),
)

signal_weights = st.fixed_dictionaries({k: weight_value for k in SIGNAL_KEYS})

KNOWN_EVIDENCE = sorted(DEFAULT_EVIDENCE_WEIGHTS.keys())


@st.composite
def prediction_dicts(draw) -> dict:
    """Generate a prediction dict shaped like the real KNN pipeline output."""
    d: dict = {}
    # Distance in cosine space [0, 2]; produces embedding_similarity in [0, 1].
    if draw(st.booleans()):
        d["distance"] = draw(
            st.floats(min_value=0.0, max_value=2.0, allow_nan=False, allow_infinity=False)
        )
    # Alignment identities can be out of [0, 1]; compute_score clamps them.
    if draw(st.booleans()):
        d["identity_nw"] = draw(
            st.one_of(
                st.none(),
                st.floats(min_value=-1.0, max_value=2.0, allow_nan=False, allow_infinity=False),
            )
        )
    if draw(st.booleans()):
        d["identity_sw"] = draw(
            st.one_of(
                st.none(),
                st.floats(min_value=-1.0, max_value=2.0, allow_nan=False, allow_infinity=False),
            )
        )
    if draw(st.booleans()):
        # Taxonomic distance is a non-negative number; large values approach 0
        # via the 1 / (1 + d) mapping.
        d["taxonomic_distance"] = draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
            )
        )
    if draw(st.booleans()):
        d["neighbor_vote_fraction"] = draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
            )
        )
    if draw(st.booleans()):
        d["evidence_code"] = draw(st.sampled_from(KNOWN_EVIDENCE + [None, "UNKNOWN"]))
    return d


def _config(
    weights: dict[str, float],
    formula: str = FORMULA_LINEAR,
    evidence_weights: dict[str, float] | None = None,
) -> ScoringConfig:
    """Mock ScoringConfig matching the shape ``compute_score`` reads."""
    cfg = MagicMock(spec=ScoringConfig)
    cfg.weights = weights
    cfg.formula = formula
    cfg.evidence_weights = evidence_weights
    return cfg


# ---------------------------------------------------------------------------
# compute_score
# ---------------------------------------------------------------------------


@given(pred=prediction_dicts(), weights=signal_weights)
def test_compute_score_is_in_unit_interval_linear(pred: dict, weights: dict) -> None:
    cfg = _config(weights, formula=FORMULA_LINEAR)
    score = compute_score(pred, cfg)
    assert 0.0 <= score <= 1.0, f"score={score} pred={pred} weights={weights}"


@given(pred=prediction_dicts(), weights=signal_weights)
def test_compute_score_is_in_unit_interval_evidence_weighted(
    pred: dict, weights: dict
) -> None:
    cfg = _config(weights, formula=FORMULA_EVIDENCE_WEIGHTED)
    score = compute_score(pred, cfg)
    assert 0.0 <= score <= 1.0


@given(
    pred=prediction_dicts(),
    weights=signal_weights,
    overrides=st.dictionaries(
        st.sampled_from(KNOWN_EVIDENCE),
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        max_size=6,
    ),
)
def test_compute_score_bounded_with_evidence_overrides(
    pred: dict, weights: dict, overrides: dict
) -> None:
    cfg = _config(weights, formula=FORMULA_LINEAR, evidence_weights=overrides)
    assert 0.0 <= compute_score(pred, cfg) <= 1.0


@given(pred=prediction_dicts(), weights=signal_weights)
def test_evidence_weighted_le_linear(pred: dict, weights: dict) -> None:
    """Evidence multiplier <= 1 → evidence_weighted score <= linear score.

    Holds because the only difference between formulas is the extra
    ``base_score *= ev_w`` multiplier on top of an already-bounded
    base_score, and ``ev_w in [0, 1]``.
    """
    lin = compute_score(pred, _config(weights, formula=FORMULA_LINEAR))
    ew = compute_score(pred, _config(weights, formula=FORMULA_EVIDENCE_WEIGHTED))
    # round(.., 6) means a tiny numeric epsilon is acceptable.
    assert ew <= lin + 1e-9, f"ew={ew} > lin={lin}, pred={pred} weights={weights}"


@given(
    pred=prediction_dicts(),
    weights=signal_weights,
    scale=st.floats(
        min_value=0.01, max_value=10.0, allow_nan=False, allow_infinity=False
    ),
)
def test_compute_score_invariant_to_uniform_weight_scaling(
    pred: dict, weights: dict, scale: float
) -> None:
    """Linear formula is a weighted average → scaling all weights does not move it.

    Edge case: when every weight is 0, scaling is also 0 and the function
    short-circuits to 0.0 regardless. ``assume`` discards those draws so we
    only exercise the actual invariant.

    Subnormal weights are excluded by ``weight_value`` itself, not here: a
    floor on the largest weight would not be enough, because the weight that
    underflows need not be the largest one. It only has to be the only weight
    whose signal is present in ``pred``.
    """
    assume(any(w > 0.0 for w in weights.values()))
    cfg_a = _config(weights, formula=FORMULA_LINEAR)
    scaled = {k: v * scale for k, v in weights.items()}
    cfg_b = _config(scaled, formula=FORMULA_LINEAR)
    s_a = compute_score(pred, cfg_a)
    s_b = compute_score(pred, cfg_b)
    assert abs(s_a - s_b) < 1e-5, f"scale={scale} a={s_a} b={s_b} pred={pred}"


@given(pred=prediction_dicts())
def test_compute_score_zero_weights_returns_zero(pred: dict) -> None:
    cfg = _config({k: 0.0 for k in SIGNAL_KEYS}, formula=FORMULA_LINEAR)
    assert compute_score(pred, cfg) == 0.0


# ---------------------------------------------------------------------------
# score_predictions
# ---------------------------------------------------------------------------


@given(
    preds=st.lists(prediction_dicts(), max_size=16),
    weights=signal_weights,
    formula=st.sampled_from([FORMULA_LINEAR, FORMULA_EVIDENCE_WEIGHTED]),
)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_score_predictions_sorted_descending(
    preds: list[dict], weights: dict, formula: str
) -> None:
    cfg = _config(weights, formula=formula)
    out = score_predictions(preds, cfg)
    scores = [row["score"] for row in out]
    assert scores == sorted(scores, reverse=True)
    # Each output score must still be in [0, 1].
    assert all(0.0 <= s <= 1.0 for s in scores)


@given(preds=st.lists(prediction_dicts(), max_size=8), weights=signal_weights)
def test_score_predictions_preserves_count(preds: list[dict], weights: dict) -> None:
    cfg = _config(weights, formula=FORMULA_LINEAR)
    out = score_predictions(preds, cfg)
    assert len(out) == len(preds)


@given(preds=st.lists(prediction_dicts(), min_size=1, max_size=8), weights=signal_weights)
def test_score_predictions_does_not_mutate_input(preds: list[dict], weights: dict) -> None:
    snapshot = [dict(p) for p in preds]
    cfg = _config(weights, formula=FORMULA_LINEAR)
    _ = score_predictions(preds, cfg)
    assert preds == snapshot


# ---------------------------------------------------------------------------
# propagate_scores_to_ancestors
# ---------------------------------------------------------------------------

go_term = st.from_regex(r"\AGO:[0-9]{7}\Z", fullmatch=True)
protein = st.from_regex(r"\A[A-NR-Z][0-9][A-Z0-9]{3}[0-9]\Z", fullmatch=True)


@st.composite
def parent_dag(draw):
    """Generate a small acyclic parent_map plus the universe of go ids."""
    terms = draw(st.lists(go_term, min_size=2, max_size=8, unique=True))
    # Order the terms; each term may pick parents only from later ones in
    # the list to guarantee acyclicity. Build map child -> set(parents).
    parents: dict[str, set[str]] = {}
    for i, child in enumerate(terms[:-1]):
        candidates = terms[i + 1 :]
        n = draw(st.integers(min_value=0, max_value=min(2, len(candidates))))
        chosen = draw(
            st.lists(
                st.sampled_from(candidates),
                min_size=n,
                max_size=n,
                unique=True,
            )
        )
        if chosen:
            parents[child] = set(chosen)
    return terms, parents


@st.composite
def scored_rows(draw):
    terms, parents = draw(parent_dag())
    proteins = draw(st.lists(protein, min_size=1, max_size=4, unique=True))
    rows = draw(
        st.lists(
            st.tuples(
                st.sampled_from(proteins),
                st.sampled_from(terms),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
            ),
            min_size=1,
            max_size=12,
        )
    )
    return parents, [
        {"protein_accession": acc, "go_id": gid, "score": s} for acc, gid, s in rows
    ]


@given(case=scored_rows())
def test_propagate_scores_idempotent(case) -> None:
    parents, rows = case
    once = propagate_scores_to_ancestors(rows, parents)
    twice = propagate_scores_to_ancestors(once, parents)
    # Order-insensitive set equality on (protein, go, score) tuples.
    as_set_once = {(r["protein_accession"], r["go_id"], r["score"]) for r in once}
    as_set_twice = {(r["protein_accession"], r["go_id"], r["score"]) for r in twice}
    assert as_set_once == as_set_twice


@given(case=scored_rows())
def test_propagate_scores_ancestor_max_invariant(case) -> None:
    """For every (protein, descendant, s), every ancestor must score >= s."""
    parents, rows = case
    propagated = propagate_scores_to_ancestors(rows, parents)

    # Build (protein, go) -> score lookup from the propagated output.
    by_pair: dict[tuple[str, str], float] = {}
    for r in propagated:
        by_pair[(r["protein_accession"], r["go_id"])] = r["score"]

    def ancestors_of(go_id: str) -> set[str]:
        seen: set[str] = set()
        stack = [go_id]
        while stack:
            node = stack.pop()
            for p in parents.get(node, ()):
                if p not in seen:
                    seen.add(p)
                    stack.append(p)
        return seen

    for r in rows:
        gid = r["go_id"]
        acc = r["protein_accession"]
        s = r["score"]
        for anc in ancestors_of(gid):
            anc_score = by_pair.get((acc, anc))
            assert anc_score is not None, (
                f"missing ancestor {anc} for {acc}/{gid}"
            )
            assert anc_score >= s - 1e-9, (
                f"ancestor {anc} score {anc_score} < descendant {gid} score {s}"
            )


@given(case=scored_rows())
def test_propagate_scores_does_not_mutate_input(case) -> None:
    parents, rows = case
    snapshot = [dict(r) for r in rows]
    _ = propagate_scores_to_ancestors(rows, parents)
    assert rows == snapshot
