"""Trivial reference :class:`Combiner` implementations (MR-0 only).

Two degenerate combiners that establish the contract and serve as the baseline
the MR-2 trained per-category combiner must beat:

* :class:`PassThroughCombiner` — the degenerate single-scorer case: the final
  score IS the first component (the existing monolithic-booster path expressed
  as one scorer). ``fit`` is a no-op.
* :class:`LinearCombiner` — the degenerate fixed-weight case: a weighted sum of
  the score vector, expressing the existing linear ``scoring_config`` as a
  combiner. Weights are supplied per category at construction; ``fit`` is a
  no-op (no learning here, that is MR-2).

Neither learns from data: the trained, OUT-OF-FOLD, calibrated per-category
combiner is MR-2. They exist so the port is exercised end-to-end and so the
"linear scoring_config and monolith are special cases" claim in ARCHITECTURE.md
is concrete, not just prose.
"""

from __future__ import annotations

from protea.core.domain.category import Category


class PassThroughCombiner:
    """Final score = the first component of the score vector.

    The degenerate single-scorer / monolithic-booster case. Independent of
    category. ``fit`` is a no-op (nothing to learn).
    """

    def fit(
        self,
        score_matrix_oof: list[list[float]],
        labels: list[int],
        category: Category,
    ) -> None:
        """No-op: the pass-through combiner has no parameters."""
        return None

    def combine(self, score_vector: list[float], category: Category) -> float:
        """Return the first component, or ``0.0`` for an empty vector."""
        return float(score_vector[0]) if score_vector else 0.0


class LinearCombiner:
    """Fixed-weight linear combination of the score vector, optionally per category.

    The degenerate linear ``scoring_config`` case. Weights are a flat list
    (applied to every category) or a ``{Category: weights}`` mapping. The
    weighted sum truncates / zero-pads to the score-vector length, so a vector
    shorter than the weight list (a scorer absent for this category) still
    combines cleanly. ``fit`` is a no-op: the weights are given, not learned.
    """

    def __init__(
        self,
        weights: list[float] | dict[Category, list[float]],
    ) -> None:
        self._weights = weights

    def _weights_for(self, category: Category) -> list[float]:
        if isinstance(self._weights, dict):
            return self._weights.get(category, [])
        return self._weights

    def fit(
        self,
        score_matrix_oof: list[list[float]],
        labels: list[int],
        category: Category,
    ) -> None:
        """No-op: the linear combiner uses the weights given at construction."""
        return None

    def combine(self, score_vector: list[float], category: Category) -> float:
        """Weighted sum of ``score_vector`` with this category's weights."""
        weights = self._weights_for(category)
        return float(sum(s * w for s, w in zip(score_vector, weights, strict=False)))


__all__ = [
    "LinearCombiner",
    "PassThroughCombiner",
]
