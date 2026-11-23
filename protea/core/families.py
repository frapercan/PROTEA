"""The registry of the three method families (ADR-D60, decision 2).

ONE ENTRY PER FAMILY, AND NO FOURTH. ``protea.core.contracts.method.Family`` is
the closed list; this module gives each member what ADR-D60 says a family is: a
registry entry with a short and a long descriptor (ADR-D58) and an introduction
whose text is the author's. ``tests/test_method_families.py`` fails when the two
disagree, so a family cannot be added to one and forgotten in the other.

THE DESCRIPTORS ARE ADR-D60's OWN WORDS. The short descriptor is the family's
slug, which already satisfies ADR-D58's rule for a name (lowercase, at most 64
characters, unique, never rewritten). The long descriptor restates the line
ADR-D60's decision 2 gives the family, which names what separates it from the
other two: where its representation comes from and how it predicts. Nothing in
it is a claim this module measured.

THE INTRODUCTIONS ARE EMPTY ON PURPOSE. ADR-D60 makes "the three introductions
written" one of the four conditions of the promotion milestone, and the
introductions are the author's text. Every entry carries the slot, and the slot
is None until he writes it; no code fills it with a placeholder, because a
placeholder would read as his and would make the milestone's condition look met.
:func:`families_without_introduction` says which slots are still empty.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from protea.core.contracts.method import Family

__all__ = [
    "FAMILIES",
    "FamilyEntry",
    "families_without_introduction",
    "family_entry",
]


@dataclass(frozen=True)
class FamilyEntry:
    """One method family as the platform names it.

    ``short_name`` and ``description`` are the two descriptors of ADR-D58.
    ``introduction`` is the author's text, or None while he has not written it.
    """

    family: Family
    short_name: str
    description: str
    introduction: str | None = None


FAMILIES: Mapping[Family, FamilyEntry] = MappingProxyType(
    {
        Family.EMBEDDING_RERANKER: FamilyEntry(
            family=Family.EMBEDDING_RERANKER,
            short_name="embedding-reranker",
            description=(
                "PLM embeddings, engineered features and a reranker: the representation "
                "is a protein language model's embedding, and a reranker orders the "
                "candidate terms using engineered features."
            ),
            introduction=None,
        ),
        Family.WORLD_MODEL: FamilyEntry(
            family=Family.WORLD_MODEL,
            short_name="world-model",
            description=(
                "Learned representations: a JEPA that predicts the future state of knowledge."
            ),
            introduction=None,
        ),
        Family.EXPLAINABLE_UNITS: FamilyEntry(
            family=Family.EXPLAINABLE_UNITS,
            short_name="explainable-units",
            description=(
                "PLM-free, with the explanation read from structure: no protein language "
                "model enters the procedure."
            ),
            introduction=None,
        ),
    }
)


def family_entry(family: Family | str) -> FamilyEntry:
    """The registry entry of ``family``, given as the enum or its slug.

    Raises:
        ValueError: when ``family`` names none of the three.
    """
    if family not in tuple(Family):
        raise ValueError(f"{family!r} is not one of {[f.value for f in Family]}")
    return FAMILIES[Family(family)]


def families_without_introduction() -> tuple[Family, ...]:
    """The families whose introduction the author has not written yet.

    ADR-D60 lists the three introductions among the conditions of the promotion
    milestone, so this is the check that condition reads.
    """
    return tuple(f for f in Family if not (FAMILIES[f].introduction or "").strip())
