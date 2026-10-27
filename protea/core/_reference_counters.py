"""Which of a reference's counters this platform stores, and under what name.

An external reference publishes an acceptance contract: the counters its own
truth came out at. Comparing ours against it is the only way to say the record
is FAITHFUL rather than merely declared, and the comparison is a mapping
problem before it is an arithmetic one, because the two record the same
quantity under different names.

THE NAMES ARE WRITTEN OUT, one pair at a time, and never matched by
resemblance. ``nk_pairs`` and ``nk_proteins`` differ by a factor of twenty on
the long window, and a mapping that paired either with our ``nk_annotations``
by substring would report agreement or disagreement with equal confidence. A
pair belongs here only when somebody has checked that the two names denote the
same count.

WHAT IS DELIBERATELY ABSENT. Seven of the reference's counters have no
counterpart in what a window stores, and they are listed rather than dropped:
a comparison that silently comes out 26 of 26 because it skipped what it could
not find is worse than one that says 26 of 33 and names the seven.
"""

from __future__ import annotations

from typing import Any, NamedTuple

#: ``reference counter -> the key an EvaluationSet's ``stats`` holds it under``.
#:
#: Checked pair by pair against the long window's record on 2026-10-27: every
#: pair below agreed, which is what made the mapping safe to write down. The
#: aliases that are NOT identity are the interesting half:
#:
#: * the reference counts ``*_pairs`` where we count ``*_annotations``: the
#:   same (protein, term) rows under two words;
#: * ``known_pairs`` is our ``known_terms_count``;
#: * ``new_exp_projected*`` is our ``projected_*``, because the projection is
#:   ours to name and theirs to describe;
#: * ``pk_known_proteins`` equals our ``pk_proteins`` on this window and is
#:   mapped anyway, because the reference computes it from the PK-known side
#:   and ours from the delta: two routes to one number is evidence, and
#:   leaving it out would discard that.
COUNTER_ALIASES: dict[str, str] = {
    "delta_proteins": "delta_proteins",
    "nk_proteins": "nk_proteins",
    "lk_proteins": "lk_proteins",
    "pk_proteins": "pk_proteins",
    "nk_pairs": "nk_annotations",
    "lk_pairs": "lk_annotations",
    "pk_pairs": "pk_annotations",
    "known_pairs": "known_terms_count",
    "removed_pairs": "removed_annotations",
    "removed_proteins": "removed_proteins",
    "pk_known_proteins": "pk_proteins",
    "new_exp_projected": "projected_entries",
    "new_exp_projected_pairs": "projected_pairs",
    "new_exp_projected_binding_mixed": "projected_binding_mixed",
    "new_exp_replaced_by": "new_exp_replaced_by",
    "new_exp_alt_id": "new_exp_alt_id",
    "new_exp_unmapped": "new_exp_unmapped",
    "new_not_alt_id": "new_not_alt_id",
    "new_not_replaced_by": "new_not_replaced_by",
    "new_not_unmapped": "new_not_unmapped",
    "old_exp_alt_id": "old_exp_alt_id",
    "old_exp_replaced_by": "old_exp_replaced_by",
    "old_exp_unmapped": "old_exp_unmapped",
    "old_not_alt_id": "old_not_alt_id",
    "old_not_replaced_by": "old_not_replaced_by",
    "old_not_unmapped": "old_not_unmapped",
}

#: Counters a reference may publish that no window stores, with the reason.
#:
#: Each one is a quantity this platform could compute and does not record, so
#: it is an absence in OUR record and not a flaw in the reference. Named here
#: so the verdict can say 26 of 33 and list the seven, and so that adding one
#: to ``stats`` is a visible change to this file rather than a silent rise in
#: the agreement count.
NOT_STORED: dict[str, str] = {
    "binding_dropped_new": "the binding rule's drops at t1 are not tallied on the window",
    "binding_dropped_old": "the binding rule's drops at t0 are not tallied on the window",
    "known_proteins": "the window stores the known PAIR count, not its protein count",
    "negated_pairs": "the NOT side's pair count is not tallied on the window",
    "negated_proteins": "the NOT side's protein count is not tallied on the window",
    "pk_known_pairs": "the PK-known pair count is not tallied on the window",
    "toi_dropped_pairs": "what the terms of interest dropped is a property of the RUN",
    "toi_terms": "the size of the term universe is a property of the RUN, which emits it",
}


class Disagreement(NamedTuple):
    """One counter the two records do not agree on."""

    counter: str
    reference: Any
    ours: Any


class Comparison(NamedTuple):
    """What comparing one reference against one window found.

    ``compared`` counts the pairs that could be read on both sides, which is
    the denominator any claim has to quote. ``absent`` are counters the
    reference published and this record does not keep; ``unknown`` are ones
    the mapping has never been told about, and they are kept apart because the
    first is a decision and the second is a gap in this file.
    """

    compared: int
    agreed: int
    disagreements: tuple[Disagreement, ...]
    absent: tuple[str, ...]
    unknown: tuple[str, ...]

    @property
    def reproduces(self) -> bool:
        """Every counter that could be compared agreed, and at least one did.

        The second half matters: a comparison that read nothing agrees with
        everything, and would publish a reproduction claim over an empty
        intersection.
        """
        return self.compared > 0 and not self.disagreements


def compare_counters(reference: dict[str, Any], stats: dict[str, Any]) -> Comparison:
    """Compare a reference's counters against a window's stored ones.

    Integers both sides: a window's ``stats`` is JSONB and a counter can come
    back as a string, so both are read through ``int`` and a value that is not
    a whole number is a disagreement rather than an exception -- the point of
    the comparison is to report, not to stop.
    """
    agreed = 0
    compared = 0
    disagreements: list[Disagreement] = []
    absent: list[str] = []
    unknown: list[str] = []
    for name, value in sorted(reference.items()):
        if name in NOT_STORED:
            absent.append(name)
            continue
        key = COUNTER_ALIASES.get(name)
        if key is None:
            unknown.append(name)
            continue
        if key not in stats:
            absent.append(name)
            continue
        compared += 1
        try:
            same = int(value) == int(stats[key])
        except (TypeError, ValueError):
            same = False
        if same:
            agreed += 1
        else:
            disagreements.append(Disagreement(name, value, stats[key]))
    return Comparison(
        compared=compared,
        agreed=agreed,
        disagreements=tuple(disagreements),
        absent=tuple(absent),
        unknown=tuple(unknown),
    )
