"""Unit tests for ``scripts/hpo_champion_score.py``.

Offline tests for the pure search-space helpers (``coarse_trials``,
``refine_trials``, the evidence presets). The live HPO loop that talks to the
PROTEA HTTP surface is exercised separately on the dev stack, not here.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from protea.infrastructure.orm.models.embedding.scoring_config import (  # noqa: E402
    DEFAULT_EVIDENCE_WEIGHTS,
    VALID_FORMULAS,
)
from scripts.hpo_champion_score import (  # noqa: E402
    EVIDENCE_PRESETS,
    GAMMAS,
    base_weight_vectors,
    coarse_trials,
    refine_trials,
)

EVIDENCE_LABELS = {
    "linear",
    "ev_uniform",
    "ev_experimental_only",
    "ev_iea_excluded",
    "ev_tierweighted",
}


def test_gammas_default_is_single_zero_point() -> None:
    # The IA prior lost in all 9 held-out cells (2026-06-12); axis collapsed.
    assert GAMMAS == (0.0,)


def test_evidence_presets_are_valid() -> None:
    labels = {label for label, _, _ in EVIDENCE_PRESETS}
    assert labels == EVIDENCE_LABELS
    known = set(DEFAULT_EVIDENCE_WEIGHTS)
    for _label, formula, weights in EVIDENCE_PRESETS:
        assert formula in VALID_FORMULAS
        if weights is not None:
            assert set(weights).issubset(known)
            assert all(0.0 <= v <= 1.0 for v in weights.values())


def test_experimental_only_and_iea_excluded_cover_every_code() -> None:
    by_label = {label: weights for label, _, weights in EVIDENCE_PRESETS}
    exp_only = by_label["ev_experimental_only"]
    iea_excl = by_label["ev_iea_excluded"]
    assert exp_only is not None and iea_excl is not None
    # Every known code is named explicitly (no silent fallback to defaults).
    assert set(exp_only) == set(DEFAULT_EVIDENCE_WEIGHTS)
    assert set(iea_excl) == set(DEFAULT_EVIDENCE_WEIGHTS)
    assert exp_only["EXP"] == 1.0 and exp_only["IEA"] == 0.0 and exp_only["IDA"] == 1.0
    assert iea_excl["IEA"] == 0.0 and iea_excl["EXP"] == 1.0


def test_coarse_grid_size_crosses_k_weights_evidence() -> None:
    ks = [3, 30]
    trials = coarse_trials(ks)
    expected = len(ks) * len(base_weight_vectors()) * len(EVIDENCE_PRESETS) * len(GAMMAS)
    assert len(trials) == expected
    # signatures are unique (the de-dup kept them all → no collisions).
    assert len({t.signature() for t in trials}) == len(trials)


def test_evidence_presets_yield_distinct_signatures() -> None:
    # Same K + same weight vector, differing only by evidence preset, must stay
    # distinct so they do not dedupe-collide into a single scoring_config.
    trials = coarse_trials([5])
    by_weights: dict[str, set[str]] = {}
    for t in trials:
        wkey = ",".join(f"{k}:{v}" for k, v in sorted(t.weights.items()))
        by_weights.setdefault(wkey, set()).add(t.signature())
    for sigs in by_weights.values():
        assert len(sigs) == len(EVIDENCE_PRESETS)
    labels = {t.evidence_label for t in trials}
    assert labels == EVIDENCE_LABELS


def test_refine_preserves_leader_evidence_preset() -> None:
    rng = random.Random(0)
    leader = next(
        t for t in coarse_trials([5]) if t.evidence_label == "ev_tierweighted"
    )
    children = refine_trials(leader, 8, rng)
    assert children
    for child in children:
        assert child.formula == leader.formula
        assert child.evidence_weights == leader.evidence_weights
        assert child.evidence_label == leader.evidence_label
