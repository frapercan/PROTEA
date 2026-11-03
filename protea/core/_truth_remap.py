"""What a t1 term is called at t0, when the ontology said so.

An annotation carries a GO id, and an id is not a term: between two releases
GO merges terms, obsoletes them and names successors, and twice in this
window's span it has even REVIVED an id it had obsoleted. So "does this t1
term have a node at t0" cannot be answered by asking whether the id is in
t0's vocabulary. The ontology published the answer and
:mod:`protea.infrastructure.orm.models.annotation.go_term_alias` records it.

THE RULE, FROM THE LAB'S ``build_remap``, clause by clause, because every
clause is a place where a reasonable alternative gives a different truth:

* what is LIVE at t0 maps to itself and is never remapped;
* ``alt_id`` first: an id that is not live at t0 and that a live t0 term
  lists as a secondary id IS that term;
* ``replaced_by`` second, and only for ids still unresolved, so a term that
  both halves could claim goes to ``alt_id``. One map per id, never two;
* exactly ONE distinct successor, or no map. Taking the first of several
  would be picking for the ontology; taking all of them would turn one
  annotation into several;
* the successor has to be live at t0. The single exception is a successor
  that is itself a secondary id, which is followed ONCE to its primary. No
  chains beyond that, so no depth limit and no cycles;
* ``consider`` is ignored. It is the ontology declining to name a successor,
  and following it would be inventing the decision it declined to make.

WHERE IT RUNS, WHICH IS THE PART THAT SURPRISED US. The remapping goes BEFORE
the projection and beats it: a term that can be remapped is never projected.
That is exactly the nine entries of the 211 -> 227 window, where PROTEA was
projecting to the ancestors terms whose successor the lab was using, and the
difference showed up as three extra proteins out of 40,531.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import NamedTuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term_alias import (
    KIND_ALT_ID,
    KIND_REPLACED_BY,
    GOTermAlias,
)


class Remap(NamedTuple):
    """``{old id: (id at t0, which statement resolved it)}``, and the raw maps.

    ``by_alias`` is what a caller applies. The two source maps travel with it
    because the rule needs ``alt_of`` a second time, to follow a successor
    that is itself a secondary id, and a caller that rebuilt it would be
    rebuilding half the rule.
    """

    by_alias: dict[str, tuple[str, str]]
    alt_of: dict[str, str]
    replaced_by: dict[str, set[str]]

    @property
    def counts(self) -> dict[str, int]:
        kinds = [kind for _, kind in self.by_alias.values()]
        return {
            "remap_alt_id": sum(1 for k in kinds if k == KIND_ALT_ID),
            "remap_replaced_by": sum(1 for k in kinds if k == KIND_REPLACED_BY),
        }


def alias_rows(session: Session, snapshot_id: uuid.UUID) -> list[tuple[str, str, str]]:
    """``[(alias, primary, kind)]`` as ``go_term_alias`` holds them."""
    return [
        (alias, primary, kind)
        for alias, primary, kind in session.execute(
            select(
                GOTermAlias.alias_go_id,
                GOTermAlias.primary_go_id,
                GOTermAlias.kind,
            ).where(GOTermAlias.ontology_snapshot_id == snapshot_id)
        ).all()
    ]


def build_remap(rows: list[tuple[str, str, str]], live: set[str]) -> Remap:
    """Resolve the two statements against what is live, in the lab's order.

    Sorted at both loops, so the map is a function of the data and not of the
    order the rows came back in. The first version of the projection learned
    that lesson the expensive way on a query with no ``ORDER BY``.
    """
    alt_of: dict[str, str] = {}
    replaced: dict[str, set[str]] = defaultdict(set)
    for alias, primary, kind in rows:
        if kind == KIND_ALT_ID:
            alt_of[alias] = primary
        elif kind == KIND_REPLACED_BY:
            replaced[alias].add(primary)

    resolved: dict[str, tuple[str, str]] = {}
    for alias, primary in sorted(alt_of.items()):
        # Live at t0 maps to itself: an id the snapshot still uses is not a
        # synonym of anything, even when some other term lists it.
        if alias not in live and primary in live:
            resolved[alias] = (primary, KIND_ALT_ID)

    for old, successors in sorted(replaced.items()):
        if old in live or old in resolved:
            continue
        reps = sorted(successors)
        if len(reps) != 1:
            # Several successors, or the same one twice through different
            # statements: the ontology did not name one, so neither do we.
            continue
        target = reps[0]
        if target not in live:
            promoted = alt_of.get(target)
            if promoted is None or promoted not in live:
                continue
            # Followed ONCE: a successor that is itself a secondary id.
            target = promoted
        resolved[old] = (target, KIND_REPLACED_BY)

    return Remap(by_alias=resolved, alt_of=alt_of, replaced_by=dict(replaced))


class Applied(NamedTuple):
    """What the remapping did to one side's direct annotations."""

    #: Terms replaced by their t0 identity, by the statement that resolved it.
    alt_id: int = 0
    replaced_by: int = 0
    #: Terms with no map at t0 and no projection available: dropped, counted.
    unmapped: int = 0
    #: Proteins whose direct set changed.
    proteins: int = 0

    def as_dict(self, side: str) -> dict[str, int]:
        return {
            f"{side}_alt_id": self.alt_id,
            f"{side}_replaced_by": self.replaced_by,
            f"{side}_unmapped": self.unmapped,
            f"{side}_remapped_proteins": self.proteins,
        }


def apply_remap(
    raw: dict[str, set[str]],
    remap: Remap,
    live_at_t0: set[str],
    spare: set[str] | None = None,
) -> Applied:
    """Rewrite ``raw`` in place into t0's id space, counting what happened.

    ``spare`` is the set of ids the caller will handle itself instead of
    dropping -- the terms the projection is going to take. Passing it is how
    the two rules compose in the right order: everything remappable is
    remapped, what is left and projectable is left alone for the projection,
    and only what neither can place is dropped.
    """
    protected = spare or set()
    alt = repl = unmapped = proteins = 0
    for protein, terms in raw.items():
        keep: set[str] = set()
        changed = False
        for term in terms:
            if term in live_at_t0:
                keep.add(term)
                continue
            mapped = remap.by_alias.get(term)
            if mapped is not None:
                target, kind = mapped
                keep.add(target)
                changed = True
                if kind == KIND_ALT_ID:
                    alt += 1
                else:
                    repl += 1
                continue
            if term in protected:
                # The projection's business, and it runs after this.
                keep.add(term)
                continue
            unmapped += 1
            changed = True
        if changed:
            proteins += 1
            raw[protein] = keep
    return Applied(alt_id=alt, replaced_by=repl, unmapped=unmapped, proteins=proteins)
