"""The t0 category: the knowledge category a method may use, read from a window's t0 side alone.

The evaluator's category of a (protein, aspect) is a property of the window's
truth, which depends on t1: both ends are trimmed to the terms alive at both,
and the NOT rows of both ends count. A method cannot know t1, so a method that
keys anything by category (a calibration stratum, a feature naming a cell, the
coverage guarantee's eligibility) keys it by the category the protein's state at
t0 gives:

- **partial knowledge** in an aspect when the protein's closed t0 experimental
  state has a term in that aspect;
- **limited knowledge** when it has one in another aspect only;
- **no knowledge** otherwise.

The state is LAFA's t0 end, built by the frame's own steps
(:func:`protea.core.lafa_frame.end_state`): the protein's direct experimental
rows at t0, each id named under the t0 ontology (alternative ids to primaries,
obsolete terms to their replacement, what is not alive dropped), the protein's
t0 NOT terms (by the window's ``not_codes``) and everything under them taken out
of its direct rows, then the closure through ``is_a`` and ``part_of`` within one
aspect. Only the rows of the given proteins are read
(:func:`protea.core._lafa_frame_data.load_end_rows` with ``accessions``).

**What it reads.** The window's ``old_annotation_set_id``, its t0 ontology
(``old_native_snapshot_id``, which is the pivot of a window of LAFA's frame) and
its ``not_codes``. **Never** its t1 corpus, its truth parquet or its buckets:
an apply step that keys by this category reads nothing a method could not read.

The categories are the evaluator's own vocabulary
(:class:`protea.core.domain.category.Category`, codes ``NK``, ``LK``, ``PK``), so
a declared threshold indexed by the evaluator's cell when it is read and by the
t0 category when a method applies it names one cell. The two agree except on
the pairs :func:`disagreement_table` counts.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from protea.core import lafa_frame
from protea.core._lafa_frame_data import load_end_ontology, load_end_rows
from protea.core.domain.aspect import Aspect
from protea.core.domain.category import CATEGORY_CODES, Category

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlalchemy.orm import Session

#: The aspect codes a category is given for, in PROTEA's canonical order.
ASPECT_CODES: tuple[str, ...] = tuple(aspect.code for aspect in Aspect)


class T0CategoryUndefined(ValueError):
    """The window does not state its t0 end alone, so no t0 category can be read from it."""


def categories_of(
    accessions: Iterable[str], state: Mapping[str, Mapping[str, Collection[str]]]
) -> dict[tuple[str, str], Category]:
    """Each protein's category in each aspect, from its closed t0 state by aspect code."""
    out: dict[tuple[str, str], Category] = {}
    for accession in sorted(set(accessions)):
        held = {aspect for aspect, terms in (state.get(accession) or {}).items() if terms}
        for aspect in ASPECT_CODES:
            if aspect in held:
                category = Category.PARTIAL_KNOWLEDGE
            elif held:
                category = Category.LIMITED_KNOWLEDGE
            else:
                category = Category.NO_KNOWLEDGE
            out[(accession, aspect)] = category
    return out


@dataclass(frozen=True)
class T0Categories:
    """The t0 category of every (protein, aspect) of some proteins of one window."""

    evaluation_set_id: str
    categories: Mapping[tuple[str, str], Category]

    def category(self, accession: str, aspect: str) -> Category:
        """The category of ``accession`` in ``aspect`` (``P``, ``F`` or ``C``)."""
        try:
            return self.categories[(accession, aspect)]
        except KeyError as exc:
            raise KeyError(
                f"protein {accession} in aspect {aspect} was not read for window "
                f"{self.evaluation_set_id}; the t0 category is read for named proteins only"
            ) from exc

    def counts(self) -> dict[str, dict[str, int]]:
        """How many proteins fall in each category, per aspect code."""
        out = {aspect: dict.fromkeys(CATEGORY_CODES, 0) for aspect in ASPECT_CODES}
        for (_, aspect), category in self.categories.items():
            out[aspect][category.code] += 1
        return out


def t0_categories(session: Session, evaluation_set: Any, accessions: Iterable[str]) -> T0Categories:
    """The t0 category of ``accessions`` on ``evaluation_set``, from its t0 side alone.

    Refuses a window that does not state t0 under t0's own ontology (any frame
    but LAFA's): its t0 side is closed under a pivot that may be t1's.
    """
    if evaluation_set.truth_profile != lafa_frame.PROFILE:
        raise T0CategoryUndefined(
            f"window {evaluation_set.id} has truth profile {evaluation_set.truth_profile!r}; "
            f"the t0 category is read on LAFA's frame ({lafa_frame.PROFILE!r}), whose t0 end "
            "is stated under t0's own ontology. Another frame closes t0 under a pivot that "
            "may be t1's, which a method may not read."
        )
    wanted = sorted(set(accessions))
    ontology = load_end_ontology(session, evaluation_set.old_native_snapshot_id)
    rows, not_rows = load_end_rows(
        session,
        evaluation_set.old_annotation_set_id,
        evaluation_set.not_codes == "experimental",
        accessions=wanted,
    )
    state, _ = lafa_frame.end_state(ontology, rows, not_rows, Counter(), "t0")
    return T0Categories(str(evaluation_set.id), categories_of(wanted, state))


def disagreement_table(
    t0: T0Categories, benchmark_pairs: Iterable[tuple[str, str, str]]
) -> dict[str, dict[str, int]]:
    """The t0 category (rows) against the evaluator's category (columns), over benchmark pairs.

    ``benchmark_pairs`` are ``(accession, aspect, evaluator category)``: each
    target in each aspect where the window's truth holds its gains, with the
    category of the bucket holding them (``NK``, ``LK`` or ``PK``). Only an
    evaluation step or a fit may read them; the table is what they record.
    """
    table = {row: dict.fromkeys(CATEGORY_CODES, 0) for row in CATEGORY_CODES}
    for accession, aspect, evaluator in benchmark_pairs:
        table[t0.category(accession, aspect).code][evaluator] += 1
    return table


def disagreements(table: Mapping[str, Mapping[str, int]]) -> int:
    """The pairs off the diagonal: where the t0 category and the evaluator's differ."""
    return sum(count for row, cells in table.items() for col, count in cells.items() if row != col)


__all__ = [
    "ASPECT_CODES",
    "T0Categories",
    "T0CategoryUndefined",
    "categories_of",
    "disagreement_table",
    "disagreements",
    "t0_categories",
]
