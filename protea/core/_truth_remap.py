"""What a t1 term is called at t0, when the ontology said so.

An annotation carries a GO id, and an id is not a term: between two releases
GO merges terms, obsoletes them and names successors, and twice in this
window's span it has even REVIVED an id it had obsoleted. So "does this t1
term have a node at t0" cannot be answered by asking whether the id is in
t0's vocabulary. The ontology published the answer and
:mod:`protea.infrastructure.orm.models.annotation.go_term_alias` records it.

WHICH SNAPSHOT THE MAP IS READ FROM, WHICH IS THE CLAUSE THAT WAS WRONG.
The alias table belongs to a snapshot, and the one to read is THE GRAPH THAT
CLOSES THE SIDE -- ``EvaluationSet.old_native_snapshot_id`` or
``new_native_snapshot_id`` -- not the one the side's corpus was loaded under.
The two coincide whenever each end closes under its own era, and they do not
when a window closes both ends under one pivot, which is the construction
this campaign uses.

Reading the corpus's own snapshot made the rule a NO-OP there, because every
annotation a corpus carries is live in the era that loaded it: clause one
answered "live, leave it alone" for all of them, nothing was remapped, and
the ids the pivot cannot name died later at ``closure & pivot_go_ids`` with
no counter saying so. Measured on an early window with a 2022-07 pivot: 772
terms over 13,259 proteins on t0 and 562 over 9,098 on t1, and the native
closure saved none of them, because an id with no row in the closing graph
has no parents there either. Read from the closing graph instead, 17,250 of
t0's entries resolve through ``alt_id`` and 68 through ``replaced_by``,
11,522 and 77 on t1, and the 6,354 that resolve to nothing are DISCARDED AND
COUNTED. The window whose corpus era IS its pivot is unaffected, in every
counter, which is what makes the change safe to make on a campaign already
measured.

THE RULE, FROM THE LAB'S ``build_remap``, clause by clause, because every
clause is a place where a reasonable alternative gives a different truth:

* what is LIVE in the closing graph maps to itself and is never remapped;
* ``alt_id`` first: an id that is not live there and that a live term of
  that same graph lists as a secondary id IS that term;
* ``replaced_by`` second, and only for ids still unresolved, so a term that
  both halves could claim goes to ``alt_id``. One map per id, never two;
* exactly ONE distinct successor, or no map. Taking the first of several
  would be picking for the ontology; taking all of them would turn one
  annotation into several;
* the successor has to be live there too. The single exception is a successor
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
    #: The resolved ids whose destination sits in a DIFFERENT aspect. The
    #: substitution happens anyway -- the ontology named that successor --
    #: and travels with the map so a caller can count it without rebuilding
    #: the aspect comparison. Empty when no aspect map was supplied.
    crossed_aspect: frozenset[str] = frozenset()

    @property
    def counts(self) -> dict[str, int]:
        kinds = [kind for _, kind in self.by_alias.values()]
        return {
            "remap_alt_id": sum(1 for k in kinds if k == KIND_ALT_ID),
            "remap_replaced_by": sum(1 for k in kinds if k == KIND_REPLACED_BY),
            "remap_aspect_crossed": len(self.crossed_aspect),
        }


class SideFrame(NamedTuple):
    """The graph that closes ONE side of a window, as the remapping reads it.

    The three travel together because they are one answer: which snapshot was
    read, what is live in it, and what it says about ids it no longer uses as
    primary. A signature taking them loose is a signature somebody can call
    with a map from one snapshot and a liveness test from another, which is
    exactly the defect this module's docstring describes.
    """

    #: ``old_native_snapshot_id`` or ``new_native_snapshot_id`` of the window.
    snapshot_id: uuid.UUID
    #: Its non-obsolete ids: what "needs no rewriting here" means.
    live: set[str]
    remap: Remap


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


def _crossings(
    resolved: dict[str, tuple[str, str]], aspect: dict[str, str]
) -> frozenset[str]:
    """The resolved ids whose destination sits in another aspect.

    Both ends have to have an aspect in the SAME graph for the comparison to
    mean anything: an id the closing graph does not carry has no aspect
    there, and calling that a crossing would be reading a third snapshot
    into a two-snapshot question.
    """
    return frozenset(
        alias
        for alias, (target, _) in resolved.items()
        if aspect.get(alias) and aspect.get(target) and aspect[alias] != aspect[target]
    )


def build_remap(
    rows: list[tuple[str, str, str]],
    live: set[str],
    aspect: dict[str, str] | None = None,
) -> Remap:
    """Resolve the two statements against what is live, in the lab's order.

    ``rows``, ``live`` and ``aspect`` must all come from the SAME snapshot,
    and that snapshot is the one that CLOSES the side the map will be applied
    to. See the module docstring: reading the corpus's own snapshot instead
    turns the whole rule into a no-op.

    ``aspect`` is optional and changes no substitution: it only fills
    :attr:`Remap.crossed_aspect`, so a caller that has no aspect map gets the
    same map with an empty counter rather than a different truth.

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

    return Remap(
        by_alias=resolved,
        alt_of=alt_of,
        replaced_by=dict(replaced),
        crossed_aspect=_crossings(resolved, aspect) if aspect else frozenset(),
    )


class Applied(NamedTuple):
    """What the remapping did to one side's direct annotations."""

    #: Terms replaced by their t0 identity, by the statement that resolved it.
    alt_id: int = 0
    replaced_by: int = 0
    #: Terms with no map in the closing graph and no projection available:
    #: dropped, counted. This is the counter the old anchoring never reached,
    #: because nothing ever failed to resolve when nothing was resolved.
    unmapped: int = 0
    #: Proteins whose direct set changed.
    proteins: int = 0
    #: Substitutions, among the above, that landed in another aspect. A subset
    #: of ``alt_id + replaced_by``, not a fourth outcome.
    aspect_crossed: int = 0

    def as_dict(self, side: str) -> dict[str, int]:
        return {
            f"{side}_alt_id": self.alt_id,
            f"{side}_replaced_by": self.replaced_by,
            f"{side}_unmapped": self.unmapped,
            f"{side}_remapped_proteins": self.proteins,
            f"{side}_aspect_crossed": self.aspect_crossed,
        }


def apply_remap(
    raw: dict[str, set[str]],
    remap: Remap,
    live: set[str],
    spare: set[str] | None = None,
) -> Applied:
    """Rewrite ``raw`` in place into the closing graph's id space.

    ``live`` is the non-obsolete ids of THAT SAME graph, and the two must not
    come from different snapshots: see the module docstring for what happens
    when they do.

    ``spare`` is the set of ids the caller will handle itself instead of
    dropping -- the terms the projection is going to take. Passing it is how
    the two rules compose in the right order: everything remappable is
    remapped, what is left and projectable is left alone for the projection,
    and only what neither can place is dropped.
    """
    protected = spare or set()
    alt = repl = unmapped = proteins = crossed = 0
    for protein, terms in raw.items():
        keep: set[str] = set()
        changed = False
        for term in terms:
            if term in live:
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
                if term in remap.crossed_aspect:
                    crossed += 1
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
    return Applied(
        alt_id=alt,
        replaced_by=repl,
        unmapped=unmapped,
        proteins=proteins,
        aspect_crossed=crossed,
    )
