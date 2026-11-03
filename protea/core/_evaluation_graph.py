"""How the GO graph is read for an evaluation.

Extracted from :mod:`protea.core.evaluation`, which was at the file-LOC
budget again. This is the group in it that answers one question: given a
snapshot, what are its terms, its aspects, its edges and the closure over
them. Everything else in that module is about annotations and deltas.

TWO LOADERS THAT LOOK LIKE ONE, AND THE DIFFERENCE IS LOAD-BEARING.
``_load_pivot_term_universe`` keeps obsolete terms, because the question it
answers is what the pivot can NAME at all, and the intersection it feeds is
about expressibility. ``_live_terms_with_aspect`` drops them, because its
question is whether there is a LIVE node to land on, and cafaeval's own graph
excludes obsolete terms: a truth that landed on one would sit on a node the
scorer does not have.
"""

from __future__ import annotations

import uuid
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session


def _load_children_map(session: Session, snapshot_id: uuid.UUID) -> dict[int, set[int]]:
    """Load GO DAG as {parent_go_term_id: {child_go_term_id}} for a snapshot.

    Only is_a and part_of relationships are used for NOT-propagation, matching
    the CAFA evaluation protocol.
    """
    rows = session.execute(
        text("""
        SELECT parent_go_term_id, child_go_term_id
        FROM go_term_relationship
        WHERE ontology_snapshot_id = :snap_id
          AND relation_type IN ('is_a', 'part_of')
    """),
        {"snap_id": snapshot_id},
    ).fetchall()

    children: dict[int, set[int]] = defaultdict(set)
    for parent_id, child_id in rows:
        children[parent_id].add(child_id)
    return dict(children)

def _get_descendants(term_id: int, children_map: dict[int, set[int]]) -> set[int]:
    """BFS to collect all descendant term IDs (exclusive of start term)."""
    visited: set[int] = set()
    queue = list(children_map.get(term_id, set()))
    while queue:
        current = queue.pop()
        if current in visited:
            continue
        visited.add(current)
        queue.extend(children_map.get(current, set()) - visited)
    return visited

def _load_parents_by_go_id(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """Return ``{child_go_id: {parent_go_id}}`` in string space (is_a + part_of)."""
    rows = session.execute(
        text("""
        SELECT child.go_id, parent.go_id
        FROM go_term_relationship rel
        JOIN go_term child ON child.id = rel.child_go_term_id
        JOIN go_term parent ON parent.id = rel.parent_go_term_id
        WHERE rel.ontology_snapshot_id = :snap_id
          AND rel.relation_type IN ('is_a', 'part_of')
    """),
        {"snap_id": snapshot_id},
    ).fetchall()
    parents: dict[str, set[str]] = defaultdict(set)
    for child_go, parent_go in rows:
        parents[child_go].add(parent_go)
    return dict(parents)

def _load_children_by_go_id(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """Return ``{parent_go_id: {child_go_id}}`` in string space (is_a + part_of)."""
    rows = session.execute(
        text("""
        SELECT parent.go_id, child.go_id
        FROM go_term_relationship rel
        JOIN go_term parent ON parent.id = rel.parent_go_term_id
        JOIN go_term child ON child.id = rel.child_go_term_id
        WHERE rel.ontology_snapshot_id = :snap_id
          AND rel.relation_type IN ('is_a', 'part_of')
    """),
        {"snap_id": snapshot_id},
    ).fetchall()
    children: dict[str, set[str]] = defaultdict(set)
    for parent_go, child_go in rows:
        children[parent_go].add(child_go)
    return dict(children)

def _bfs_closure(seeds: set[str], edges: dict[str, set[str]]) -> set[str]:
    """BFS over ``edges`` from ``seeds``, returning the inclusive closure."""
    closure: set[str] = set(seeds)
    queue: list[str] = list(seeds)
    while queue:
        cur = queue.pop()
        for nxt in edges.get(cur, ()):
            if nxt not in closure:
                closure.add(nxt)
                queue.append(nxt)
    return closure

def _load_pivot_term_universe(
    session: Session, pivot_snapshot_id: uuid.UUID
) -> tuple[set[str], dict[str, str]]:
    """Return ``(set of pivot go_ids with aspect, {go_id: aspect})``.

    Terms without an aspect are excluded — they cannot participate in CAFA
    namespace bucketing and would otherwise leak through the intersect step.
    """
    rows = session.execute(
        text("""
        SELECT go_id, aspect FROM go_term
        WHERE ontology_snapshot_id = :snap_id AND aspect IS NOT NULL
    """),
        {"snap_id": pivot_snapshot_id},
    ).fetchall()
    go_ids: set[str] = set()
    aspect_by_go_id: dict[str, str] = {}
    for go_id, aspect in rows:
        go_ids.add(go_id)
        aspect_by_go_id[go_id] = aspect
    return go_ids, aspect_by_go_id

def _live_terms_with_aspect(
    session: Session, snapshot_id: uuid.UUID
) -> tuple[set[str], dict[str, str]]:
    """The NON-obsolete ids of a snapshot, with their aspects.

    The pivot universe loader next to this one deliberately keeps obsolete
    terms: the intersection it feeds is about what the pivot can name at all.
    The projection asks a different question, "is there a live node at t0",
    and an obsolete term is not one -- cafaeval's own graph excludes them, so
    projecting onto one would land on a node the scorer does not have.

    Twinned with ``_run_cafa_setup._live_go_ids``, which asks the same
    question through the ORM for the terms of interest. That module imports
    this one, so the two cannot be merged without a cycle; they are two
    readings of one predicate, ``is_obsolete = false``.
    """
    rows = session.execute(
        text("""
        SELECT go_id, aspect FROM go_term
        WHERE ontology_snapshot_id = :snap_id
          AND aspect IS NOT NULL
          AND is_obsolete = false
    """),
        {"snap_id": snapshot_id},
    ).fetchall()
    return {go_id for go_id, _ in rows}, {go_id: aspect for go_id, aspect in rows}
