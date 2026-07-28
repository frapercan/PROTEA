"""Ground truth is first appearance, not the difference between two endpoints.

An annotation counts as ground truth for a window if it is present at the end
**and was never present at any earlier cut**. The pairwise difference between
the window's two endpoints is retired.

**Why the pairwise rule is wrong here.** The corpus contracts as well as grows.
It has lost roughly thirty percent of its volume twice in the release history
this campaign spans. So an annotation can be present early, be withdrawn, and
return. Under a pairwise difference it then counts as new, and the method is
asked to predict something already known and already sitting in its own
training corpus. That is not a hard case scored generously, it is a leak.

**What was measured.** Over eleven consecutive releases, as much as 63.7% of
apparent additions on all-evidence data had been seen before. On experimental
evidence, which is the operating regime, the rate falls to about one percent.
One percent would be tolerable if it were uniform, and it is not: the leak
tracks the contraction points, and the validation series crosses one. A single
global correction would be wrong exactly where it matters most.

**What it costs and what it buys.** It needs the release history rather than
two endpoints, which the run re-ingests anyway. In exchange the restoration
rate becomes a published property of the corpus in its own right, which is a
result rather than a caveat, and it is the version that survives a reader who
looks at the release table.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

__all__ = [
    "NotEnoughHistoryError",
    "RestorationReport",
    "Snapshot",
    "first_appearance",
    "pairwise_additions",
    "restoration_report",
]

#: One corpus cut: ``{protein: {namespace: {go_id}}}``. The same shape the
#: evaluation loaders already produce, so a caller does not reshape anything.
Snapshot = dict[str, dict[str, set[str]]]


class NotEnoughHistoryError(ValueError):
    """First appearance needs the history, and it was not supplied.

    Raised rather than falling back to a pairwise difference over whatever was
    given. A silent fallback is the failure this module exists to prevent: it
    would produce a ground truth of the expected shape that is wrong by the
    restoration rate, with nothing to indicate which rule had been applied.
    """


def _pairs(snapshot: Snapshot) -> set[tuple[str, str, str]]:
    """Flatten a cut into ``(protein, namespace, go_id)`` triples."""
    return {
        (protein, namespace, go_id)
        for protein, by_ns in snapshot.items()
        for namespace, terms in by_ns.items()
        for go_id in terms
    }


def _nest(pairs: Iterable[tuple[str, str, str]]) -> Snapshot:
    """Rebuild the nested shape from triples."""
    out: Snapshot = {}
    for protein, namespace, go_id in pairs:
        out.setdefault(protein, {}).setdefault(namespace, set()).add(go_id)
    return out


def pairwise_additions(previous: Snapshot, latest: Snapshot) -> Snapshot:
    """What the retired rule would have called new: present now, absent before.

    Kept, and exported, because the restoration rate is the difference between
    this and :func:`first_appearance`. A quantity nobody can compute is a
    quantity nobody can check.
    """
    return _nest(_pairs(latest) - _pairs(previous))


def first_appearance(history: Sequence[Snapshot]) -> Snapshot:
    """Return the annotations appearing for the first time at the last cut.

    ``history`` is ordered oldest first and must hold at least two cuts. Every
    cut before the last one counts as prior knowledge of the annotation, not
    just the one immediately before it, which is the whole difference from the
    retired rule.
    """
    if len(history) < 2:
        raise NotEnoughHistoryError(
            f"first appearance needs at least two cuts, got {len(history)}. "
            f"With fewer, every annotation looks new and the ground truth is "
            f"the whole corpus. Supply the release history rather than the "
            f"window's endpoints."
        )
    ever_seen: set[tuple[str, str, str]] = set()
    for snapshot in history[:-1]:
        ever_seen |= _pairs(snapshot)
    return _nest(_pairs(history[-1]) - ever_seen)


@dataclass(frozen=True)
class RestorationReport:
    """How much of the apparent growth is the corpus returning what it removed.

    ``restored`` are the annotations the retired rule would have counted as new
    which had in fact been present at some earlier cut. Reported per namespace
    as well as in total, because the rate is not uniform: it tracks the
    contraction points, so a single global figure would mislead exactly where
    the validation series crosses one.
    """

    apparent_additions: int
    genuine_first_appearances: int
    restored: int
    restored_by_namespace: dict[str, int] = field(default_factory=dict)
    apparent_by_namespace: dict[str, int] = field(default_factory=dict)

    @property
    def restoration_rate(self) -> float:
        """Share of apparent additions that had been seen before, in [0, 1]."""
        if self.apparent_additions == 0:
            return 0.0
        return self.restored / self.apparent_additions

    def rate_for_namespace(self, namespace: str) -> float:
        """The same share within one sub-ontology."""
        apparent = self.apparent_by_namespace.get(namespace, 0)
        if apparent == 0:
            return 0.0
        return self.restored_by_namespace.get(namespace, 0) / apparent

    def as_dict(self) -> dict[str, object]:
        """A flat mapping for emitting alongside a job's events."""
        return {
            "apparent_additions": self.apparent_additions,
            "genuine_first_appearances": self.genuine_first_appearances,
            "restored": self.restored,
            "restoration_rate": round(self.restoration_rate, 6),
            "restored_by_namespace": dict(self.restored_by_namespace),
            "apparent_by_namespace": dict(self.apparent_by_namespace),
        }


def restoration_report(history: Sequence[Snapshot]) -> RestorationReport:
    """Measure how much of the last cut's apparent growth is a restoration.

    This is the number that makes the choice of rule auditable. It is meant to
    be published per release beside the metric series, not computed once and
    quoted, because the rate moves with the corpus and is highest exactly where
    the series crosses a contraction.
    """
    if len(history) < 2:
        raise NotEnoughHistoryError(
            f"the restoration rate needs at least two cuts, got {len(history)}"
        )
    apparent = _pairs(history[-1]) - _pairs(history[-2])
    genuine = _pairs(history[-1])
    for snapshot in history[:-1]:
        genuine -= _pairs(snapshot)
    restored = apparent - genuine

    apparent_by_ns: dict[str, int] = {}
    restored_by_ns: dict[str, int] = {}
    for _, namespace, _go in apparent:
        apparent_by_ns[namespace] = apparent_by_ns.get(namespace, 0) + 1
    for _, namespace, _go in restored:
        restored_by_ns[namespace] = restored_by_ns.get(namespace, 0) + 1

    return RestorationReport(
        apparent_additions=len(apparent),
        genuine_first_appearances=len(genuine),
        restored=len(restored),
        restored_by_namespace=restored_by_ns,
        apparent_by_namespace=apparent_by_ns,
    )
