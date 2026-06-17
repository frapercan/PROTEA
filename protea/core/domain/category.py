"""CAFA evaluation category (NK / LK / PK) for a ``(protein, candidate)`` pair.

The CAFA benchmark partitions every ``(query protein, candidate aspect)`` into
three categories, derived from the protein's pre-cutoff EXPERIMENTAL annotations
(the t0 "known" terms):

* **NK** (No-Knowledge): the protein has NO t0 experimental GO annotation in
  ANY aspect.
* **LK** (Limited-Knowledge): the protein HAS a t0 experimental annotation in
  some aspect but NOT in the aspect of this candidate term.
* **PK** (Partial-Knowledge): the protein HAS a t0 experimental annotation in
  the aspect of this candidate term.

Until this enum landed, the three codes circulated as bare ``"NK"`` / ``"LK"`` /
``"PK"`` string literals (see ``_category_dispatch.CATEGORY_*`` and the
per-category booster keys). The meta-reranker ports (``EvidenceScorer`` declares
``applies_to: set[Category]``, the ``Combiner`` is per category) want a single
typed source of truth, so this enum mirrors :class:`protea.core.domain.aspect.Aspect`.
The existing string literals stay valid: :meth:`Category.from_code` /
:attr:`Category.code` convert at the boundary, and ``CATEGORY_CODES`` matches the
historical ``_category_dispatch.CATEGORIES`` tuple element-for-element.

Typical usage::

    from protea.core.domain.category import Category

    # priors do not apply to No-Knowledge proteins
    applies_to = {Category.LK, Category.PK}

    # convert from a dispatch string
    cat = Category.from_code(rec["category"])
"""

from __future__ import annotations

from enum import Enum


class Category(Enum):
    """CAFA No-Knowledge / Limited-Knowledge / Partial-Knowledge partition.

    Iteration order is the canonical PROTEA order (NK -> LK -> PK), matching the
    historical ``_category_dispatch.CATEGORIES`` tuple this enum complements.
    """

    NO_KNOWLEDGE = "NK"
    LIMITED_KNOWLEDGE = "LK"
    PARTIAL_KNOWLEDGE = "PK"

    @property
    def code(self) -> str:
        """Two-char wire code (``"NK"`` / ``"LK"`` / ``"PK"``).

        The format used by ``_category_dispatch`` and the per-category booster
        keys. Use this when comparing against a ``rec["category"]`` value or a
        per-category artifact key.
        """
        return self.value

    @classmethod
    def from_code(cls, code: str) -> Category:
        """Build a Category from its two-char wire code."""
        return _CODE_TO_CATEGORY[code]


_CODE_TO_CATEGORY: dict[str, Category] = {c.code: c for c in Category}


# Canonical iteration tuple, kept as a module-level constant so callers that
# destructure into ``for code in CATEGORY_CODES`` keep working without importing
# the enum. Element-for-element equal to ``_category_dispatch.CATEGORIES``.
CATEGORY_CODES: tuple[str, str, str] = tuple(c.code for c in Category)  # type: ignore[assignment]


__all__ = [
    "CATEGORY_CODES",
    "Category",
]
