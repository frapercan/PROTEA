"""The refusals every door into the universe applies to what it reads (ADR-D60).

The universe refuses a cut after the mark, any UniProt release from the
ceiling on, and any source published after the cut it serves, both where it is
built and where it is loaded. The two values live in :mod:`protea.core.marks`;
the three checks live here, so every loader asks them the same way and words
the refusal the same way.

Standard library only, like the mark itself, so the code that runs away from
the server can import it with no database stack.
"""

from __future__ import annotations

from datetime import date

from protea.core.marks import MARK_RELEASE, UNIPROT_CEILING

__all__ = [
    "SourceGuardError",
    "is_at_or_after_the_uniprot_ceiling",
    "refuse_a_cut_after_the_mark",
    "refuse_a_dated_source",
    "refuse_a_source_published_after_its_cut",
    "refuse_a_uniprot_release_at_or_after_the_ceiling",
    "uniprot_release_key",
]


class SourceGuardError(ValueError):
    """A source, or a cut, the universe is not allowed to read."""


def uniprot_release_key(release: str) -> tuple[int, int]:
    """``(year, number)`` of a UniProt release name such as ``2025_03``.

    Compared as numbers and never as text, so the order holds whatever the
    padding of the number. A name of any other shape is refused: a release the
    guard cannot place cannot be shown to come before the ceiling.
    """
    year, separator, number = release.strip().partition("_")
    if not (separator and len(year) == 4 and year.isdigit() and number.isdigit()):
        raise SourceGuardError(f"{release!r} is not a UniProt release name such as '2025_03'")
    return int(year), int(number)


def is_at_or_after_the_uniprot_ceiling(release: str) -> bool:
    """Whether a UniProt (or UniRef) release is one the universe may not read."""
    return uniprot_release_key(release) >= uniprot_release_key(UNIPROT_CEILING)


def refuse_a_uniprot_release_at_or_after_the_ceiling(
    release: str, *, source: str = "UniProt"
) -> None:
    """Refuse a UniProt release from the ceiling on, naming the source as given."""
    if is_at_or_after_the_uniprot_ceiling(release):
        raise SourceGuardError(
            f"{source} {release} is at or after {UNIPROT_CEILING}, the first UniProt "
            f"release the universe may not read: it lies past the mark {MARK_RELEASE}, "
            "whose test was spent"
        )


def refuse_a_cut_after_the_mark(cut_release: int) -> None:
    """Refuse a cut whose GOA release is after the mark."""
    if cut_release > MARK_RELEASE:
        raise SourceGuardError(f"cut {cut_release} is after the mark {MARK_RELEASE}")


def refuse_a_source_published_after_its_cut(
    *, source: str, published: date, cut_date: date
) -> None:
    """Refuse a source published after the cut it serves; the same day is allowed."""
    if published > cut_date:
        raise SourceGuardError(
            f"{source} was published on {published.isoformat()}, after the cut it "
            f"serves ({cut_date.isoformat()})"
        )


def refuse_a_dated_source(
    *, source: str, published: date, cut_release: int, cut_date: date
) -> None:
    """Both refusals for a source dated against a cut, the mark first."""
    refuse_a_cut_after_the_mark(cut_release)
    refuse_a_source_published_after_its_cut(source=source, published=published, cut_date=cut_date)
