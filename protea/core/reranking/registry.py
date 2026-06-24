"""``ScorerRegistry`` — the hexagonal seam for evidence scorers.

A dict-backed registry resolving :class:`EvidenceScorer` instances by name,
mirroring :class:`protea.core.contracts.registry.OperationRegistry`. This is the
seam the UI + runs configure against (which scorers are active per run, per
category) without importing concrete scorer classes. MR-4 surfaces it in the UI;
MR-2 resolves the combiner's input columns through it.
"""

from __future__ import annotations

from collections.abc import Iterator

from protea.core.domain.category import Category
from protea.core.reranking.ports import EvidenceScorer


class ScorerRegistry:
    """Register / resolve :class:`EvidenceScorer` instances by name."""

    def __init__(self) -> None:
        self._scorers: dict[str, EvidenceScorer] = {}

    def register(self, scorer: EvidenceScorer) -> None:
        """Register ``scorer`` under its ``name``; reject duplicates."""
        if scorer.name in self._scorers:
            raise ValueError(f"Scorer already registered: {scorer.name}")
        self._scorers[scorer.name] = scorer

    def get(self, name: str) -> EvidenceScorer:
        """Resolve a scorer by name, or raise ``KeyError``."""
        try:
            return self._scorers[name]
        except KeyError as e:
            raise KeyError(f"Unknown scorer: {name}") from e

    def names(self) -> list[str]:
        """Registered scorer names in insertion (registration) order."""
        return list(self._scorers)

    def for_category(self, category: Category) -> list[EvidenceScorer]:
        """Registered scorers that apply to ``category``, in registration order.

        The per-category combiner (MR-2) builds its input vector from exactly
        this list, so a scorer that does not apply (e.g. a prior on NK) never
        contributes a column for that category.
        """
        return [s for s in self._scorers.values() if category in s.applies_to]

    def __contains__(self, name: object) -> bool:
        return name in self._scorers

    def __iter__(self) -> Iterator[EvidenceScorer]:
        return iter(self._scorers.values())

    def __len__(self) -> int:
        return len(self._scorers)


__all__ = [
    "ScorerRegistry",
]
