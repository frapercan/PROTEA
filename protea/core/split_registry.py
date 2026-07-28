"""The temporal splits, the ground-truth rule and the exclusion rule, in one place.

A *split* is a named set of evaluation windows, and a *window* is an ordered
pair of corpus releases. Three splits exist and their names invert the usual
convention on purpose:

* **train** is the earlier cuts, freely partitioned, and informs model fitting.
* **adjustment** is the windows before the board's mark. It informs every
  hyperparameter, every threshold, every design choice and the choice of
  champion. What machine learning usually calls validation is called adjustment
  here, because adjusting and selecting is all it does.
* **validation** is the board's window and everything after it. It is scored by
  the board, and it informs **nothing**. It is reported and never optimised
  against, which is what makes it an external validation rather than a second
  selection set.

There is deliberately no internal held-out test. Inventing one would mean
holding out twice and reporting whichever came out better.

Two rules travel with the splits because they are meaningless apart from them,
and leaving either as an undeclared implementation detail is how both have
gone wrong before:

* **Ground truth is first appearance.** An annotation counts for a window if it
  is present at the end **and was never present at any earlier cut**. The
  pairwise difference is retired; see :func:`ground_truth_requires_history`.
* **Exclusion is what the protein knew at the start.** For prior-knowledge
  cells only, the withheld terms are the protein's full set at the window's
  start, including terms the corpus has since withdrawn. See
  :func:`exclusion_basis`.

Resolution raises rather than guesses. A split whose windows are not yet
decided raises :class:`SplitUndecidedError` instead of returning a default,
because a registry that silently answers with a placeholder is worse than one
that has no answer: the placeholder propagates into results that look decided.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

__all__ = [
    "BOARD_MARK",
    "RELEASES",
    "ExclusionBasis",
    "Release",
    "ReleaseWindow",
    "Split",
    "SplitLeakError",
    "SplitName",
    "SplitUndecidedError",
    "UnknownReleaseError",
    "UnknownSplitError",
    "COMPARABLE_WINDOW",
    "adjustment_candidates",
    "menu_is_sufficient",
    "assert_may_inform",
    "consecutive_windows",
    "exclusion_basis",
    "ground_truth_requires_history",
    "release",
    "releases_before",
    "resolve_split",
    "windows_for",
]


class UnknownSplitError(KeyError):
    """A split was requested by a name the registry does not define."""


class UnknownReleaseError(KeyError):
    """A release was requested by an identifier the registry does not define."""


class SplitUndecidedError(RuntimeError):
    """A split's windows are not decided yet, and no default may stand in.

    Raised rather than returning a placeholder. The decision this guards is
    which windows are representative enough to select a champion on, and it
    cannot be made until additions and removals have been decomposed per
    release. A placeholder here would produce champions selected against an
    unexamined window and reported as though the choice had been made.
    """


class SplitLeakError(RuntimeError):
    """Something that may not inform a decision was used to inform one.

    The validation split exists to be reported, never optimised against. If it
    reaches a threshold, a hyperparameter or a champion choice, the external
    validation is destroyed by the very mechanism the temporal design exists to
    prevent, and no later result restores it.
    """


@dataclass(frozen=True, order=True)
class Release:
    """One published corpus release.

    ``published`` is what orders releases. The identifier is opaque on purpose:
    ordering derived from the identifier would break the moment the upstream
    naming changed, and the dates are what the temporal design actually means.
    """

    published: date
    identifier: str


# The published release history. Ordering, adjacency and every window in this
# module are derived from this one table, so a new release is added here and
# nowhere else.
RELEASES: tuple[Release, ...] = tuple(
    sorted(
        (
            Release(date(2025, 5, 3), "v226"),
            Release(date(2025, 9, 4), "v227"),
            Release(date(2025, 11, 10), "v228"),
            Release(date(2025, 12, 4), "v229"),
            Release(date(2026, 3, 4), "v230"),
            Release(date(2026, 4, 10), "v231"),
            Release(date(2026, 4, 30), "v232"),
            Release(date(2026, 6, 2), "v233"),
            Release(date(2026, 6, 17), "v234"),
        )
    )
)

#: The release the board scored. Everything from here forward is validation and
#: informs nothing; everything strictly earlier is available for fitting and
#: for selection.
BOARD_MARK = "v227"


def release(identifier: str) -> Release:
    """Return the release with this identifier, or raise."""
    for rel in RELEASES:
        if rel.identifier == identifier:
            return rel
    known = ", ".join(r.identifier for r in RELEASES)
    raise UnknownReleaseError(f"no release {identifier!r} in the registry; known: {known}")


def releases_before(identifier: str) -> tuple[Release, ...]:
    """Return every release published strictly before this one, in order."""
    mark = release(identifier)
    return tuple(r for r in RELEASES if r.published < mark.published)


@dataclass(frozen=True)
class ReleaseWindow:
    """An ordered pair of releases: the state at the start and at the end.

    The window is the unit every split is built from, and the unit both rules
    below are stated against. ``start`` is where a protein's prior knowledge is
    read from and ``end`` is where ground truth is read at, subject to the
    first-appearance rule.
    """

    start: str
    end: str

    def __post_init__(self) -> None:
        first, last = release(self.start), release(self.end)
        if first.published >= last.published:
            raise ValueError(
                f"window {self.start}->{self.end} is not ordered: "
                f"{self.start} was published {first.published}, {self.end} {last.published}"
            )

    @property
    def elapsed_days(self) -> int:
        """Days the window spans.

        Reported beside every per-window rate. The gaps between releases range
        from about two weeks to four months, so an unnormalised count of
        additions says more about the calendar than about the corpus.
        """
        return (release(self.end).published - release(self.start).published).days

    def __str__(self) -> str:
        return f"{self.start}->{self.end}"


def consecutive_windows(first: str, last: str) -> tuple[ReleaseWindow, ...]:
    """Return the chain of adjacent windows from ``first`` to ``last``.

    Derived from the release table rather than written out, so the chain cannot
    drift from the history it claims to cover.
    """
    lo, hi = release(first).published, release(last).published
    if lo >= hi:
        raise ValueError(f"{first} is not published before {last}")
    spanned = [r for r in RELEASES if lo <= r.published <= hi]
    return tuple(
        ReleaseWindow(a.identifier, b.identifier) for a, b in zip(spanned, spanned[1:], strict=False)
    )


class SplitName(StrEnum):
    TRAIN = "train"
    ADJUSTMENT = "adjustment"
    VALIDATION = "validation"


@dataclass(frozen=True)
class Split:
    """One named split: its windows, who scores them, and what they may inform."""

    name: SplitName
    scored_by: str
    may_inform: frozenset[str]
    balanced: bool
    windows: tuple[ReleaseWindow, ...] | None
    undecided_because: str | None = None

    def __post_init__(self) -> None:
        if (self.windows is None) == (self.undecided_because is None):
            raise ValueError(
                f"split {self.name.value} must either have windows or say why it has none"
            )


# The board's own window and everything after it. Decided, because it is not
# ours to choose: it is wherever the board's mark falls and whatever has been
# published since. Reported as a series, one point per release, crossing the
# corpus contraction rather than stopping short of it, because a curve that
# shows the discontinuity is more defensible than a single number that hides it.
#
# The series STARTS at the window ending at the board's mark, not at the mark.
# That window is the one the board scored, so it is the only point comparable
# to anyone else's number, and a series that excluded it would report no
# comparable point at all. It also fixes where the adjustment set has to stop:
# strictly earlier, or the champion would be selected on the window it is then
# validated against.
_VALIDATION_WINDOWS = consecutive_windows(
    RELEASES[RELEASES.index(release(BOARD_MARK)) - 1].identifier,
    RELEASES[-1].identifier,
)

_SPLITS: dict[SplitName, Split] = {
    SplitName.TRAIN: Split(
        name=SplitName.TRAIN,
        scored_by="ours",
        may_inform=frozenset({"model_fitting"}),
        balanced=True,
        windows=None,
        undecided_because=(
            "The train windows are the cuts earlier than the adjustment set, and "
            "the release table below begins at the base release. Every window it "
            "can currently express ends at or after the board's mark, so there is "
            "nothing earlier to partition. Ingest the releases preceding the base "
            "release, add them to the table, then partition freely."
        ),
    ),
    SplitName.ADJUSTMENT: Split(
        name=SplitName.ADJUSTMENT,
        scored_by="ours",
        may_inform=frozenset(
            {"model_fitting", "hyperparameters", "thresholds", "design", "champion_choice"}
        ),
        balanced=True,
        windows=None,
        undecided_because=(
            "The adjustment windows are chosen for representativeness, and nobody "
            "knows which are representative until additions and removals have been "
            "decomposed per release. The obvious choice, the single window ending "
            "at the board's mark, is the worst available: that window is one of the "
            "two roughly thirty percent corpus contractions, so selecting champions "
            "on it would tune against an anomaly and then validate on normal "
            "accretion. Decide it from the decomposition, then write the windows "
            "here."
        ),
    ),
    SplitName.VALIDATION: Split(
        name=SplitName.VALIDATION,
        scored_by="the board",
        may_inform=frozenset(),
        # Never balanced. The metric here is computed over a real population and
        # reweighting it makes the number incomparable to anyone else's.
        balanced=False,
        windows=_VALIDATION_WINDOWS,
    ),
}

#: The window other methods are scored on, and therefore the only point in the
#: validation series that is comparable to anyone else's number. Every other
#: point is characterisation and is labelled as such, because a series with no
#: designated headline invites the reader to choose the flattering point.
COMPARABLE_WINDOW = _VALIDATION_WINDOWS[0]


def resolve_split(name: str | SplitName) -> Split:
    """Return the split by name, or raise :class:`UnknownSplitError`."""
    try:
        key = SplitName(name)
    except ValueError:
        known = ", ".join(s.value for s in SplitName)
        raise UnknownSplitError(f"no split {name!r}; known splits: {known}") from None
    return _SPLITS[key]


def windows_for(name: str | SplitName) -> tuple[ReleaseWindow, ...]:
    """Return a split's windows, or raise if the choice has not been made.

    Never returns a default. See :class:`SplitUndecidedError`.
    """
    split = resolve_split(name)
    if split.windows is None:
        raise SplitUndecidedError(
            f"the {split.name.value} windows are not decided. {split.undecided_because}"
        )
    return split.windows


def adjustment_candidates() -> tuple[ReleaseWindow, ...]:
    """Return the windows the adjustment set may be chosen from.

    Every window that ends at or before the board's mark. This is the menu, not
    the choice: picking from it is the decision :class:`SplitUndecidedError`
    guards, and a caller that treats the whole menu as the adjustment set has
    made that decision by accident.

    The cut-off is the start of the comparable window, not the board's mark. A
    window that ended at the mark would be the very window the board scored, so
    a champion selected on it would then be validated against it.

    The menu is currently **empty**, and that is not a defect in this function,
    it is the finding: the release table begins at the comparable window's own
    start, so nothing earlier exists to select on. **A representative adjustment
    set cannot be assembled from the releases currently in the table.** Releases
    preceding it have to be ingested first. :func:`menu_is_sufficient` is the
    check to gate on rather than reading this docstring.
    """
    cutoff = release(COMPARABLE_WINDOW.start).published
    return tuple(
        w
        for w in consecutive_windows(RELEASES[0].identifier, RELEASES[-1].identifier)
        if release(w.end).published <= cutoff
    )


def menu_is_sufficient() -> bool:
    """Whether the release table can express a representative adjustment set.

    False while :func:`adjustment_candidates` offers fewer than two windows,
    because a set of one is the single window ending at the board's mark, and
    the campaign rules that one out explicitly: it is one of the two roughly
    thirty percent corpus contractions, so selecting champions on it tunes
    against an anomaly and then validates on normal accretion.
    """
    return len(adjustment_candidates()) >= 2


def assert_may_inform(name: str | SplitName, decision: str) -> None:
    """Raise if this split may not inform this kind of decision.

    The call exists so that the prohibition is enforced where decisions are
    made rather than stated in a document nobody reads at the moment it
    matters. ``decision`` is one of the tokens in a split's ``may_inform``.
    """
    split = resolve_split(name)
    if decision not in split.may_inform:
        permitted = ", ".join(sorted(split.may_inform)) or "nothing"
        raise SplitLeakError(
            f"the {split.name.value} split may not inform {decision!r}; "
            f"it may inform: {permitted}. It is scored by {split.scored_by} and "
            f"is reported, never optimised against."
        )


def ground_truth_requires_history() -> bool:
    """Ground truth is first appearance, so a window's endpoints are not enough.

    An annotation counts for a window if it is present at the end **and was
    never present at any earlier cut**. Under the retired pairwise difference an
    annotation that was present early, withdrawn, and restored counted as new,
    and the method was asked to predict something already known and already
    sitting in its training corpus.

    A probe over eleven consecutive releases measured as much as 63.7% of
    apparent additions on all-evidence data as previously seen. On experimental
    evidence, the operating regime, the rate falls to about one percent, which
    would be tolerable if it were uniform. It is not: the leak tracks the
    contraction points, and the validation series crosses one, so a single
    global correction would be wrong exactly where it matters.

    Returns True unconditionally. It is a function rather than a constant so
    that a builder reading the endpoints only has something to fail against.
    """
    return True


class ExclusionBasis(StrEnum):
    """Where the terms withheld from scoring come from."""

    #: The protein's full set at the window's start, including terms the corpus
    #: has since withdrawn.
    KNOWN_AT_START = "known_at_start"
    #: Nothing is withheld.
    NONE = "none"


def exclusion_basis(category: str) -> ExclusionBasis:
    """Return which terms are withheld from scoring for this category.

    Withholding applies to the **prior-knowledge cells only**. Omitting it
    there overstated every prior-knowledge result once, and applying it
    elsewhere would withhold terms from categories whose definition does not
    include them.

    The basis is the protein's full set at the window's start, including terms
    the corpus has since withdrawn. Intersecting with what still counts at the
    end would penalise the method for predicting something that was true at the
    start and that the corpus later removed, which is a corpus event and not a
    modelling error. The count of withdrawn-from-known is reported per release,
    because without it the dip the validation series shows at the contraction is
    unattributable, and this mechanism is the largest single effect in the
    scoring frame.
    """
    return ExclusionBasis.KNOWN_AT_START if category.upper() == "PK" else ExclusionBasis.NONE
