"""CAFA-style evaluation data computation.

This module computes the ground-truth delta between two AnnotationSets
(old → new) following the official CAFA5 evaluation protocol:

  1. Experimental evidence codes only (EXP, IDA, IMP, …)
  2. NOT-qualifier annotations are excluded — including their GO descendants
     propagated transitively through the is_a / part_of DAG.
  3. Classification is per (protein, namespace), not globally per protein:

     NK  — protein had NO experimental annotations in ANY namespace at t0.
            All novel terms across all namespaces are ground truth.

     LK  — protein had annotations in SOME namespaces at t0, but NOT in
            namespace S.  Novel terms in S are ground truth for LK.

     PK  — protein had annotations in namespace S at t0 AND gained new terms
            in S at t1.  Novel terms in S are ground truth for PK; old terms
            in S are the ``-known`` file for the CAFA evaluator.

  Note: the same protein can be LK in one namespace and PK in another
  simultaneously (e.g. had MFO+BPO at t0, gains CCO → LK in CCO, gains new
  BPO → PK in BPO).

When the two annotation sets use different OntologySnapshots,
``compute_evaluation_data_reconciled`` implements the CAFA reconciliation
protocol: propagate ancestors under each side's native DAG, intersect with a
frozen pivot snapshot, then re-propagate is handled by downstream cafaeval
(idempotent under closure semantics). Reference implementation:
``anphan0828/democafa_package``, ``utils/ontology.filter_terms_given_obo``.

Output format (matching CAFA evaluator): 2-column TSV, no header.
  protein_accession \\t go_id
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._binding_rule import PROTEIN_BINDING, drop_binding_only_mfo
from protea.core._evaluation_graph import (
    _bfs_closure,
    _get_descendants,
    _live_terms_with_aspect,
    _load_children_by_go_id,
    _load_children_map,
    _load_parents_by_go_id,
    _load_pivot_term_universe,
)

# Re-exported so callers keep importing these from here. The routing branch
# lives in its own module to keep this file inside the section 3 file-LOC
# budget and to give the defect it closes room to be explained.
from protea.core._evaluation_snapshot_routing import (
    SnapshotMismatchError as SnapshotMismatchError,
)
from protea.core._evaluation_snapshot_routing import (
    compute_evaluation_data_for_sets as compute_evaluation_data_for_sets,
)
from protea.core._truth_projection import Graph, Projection, project_direct_annotations
from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL

# Parquet column for the bucket each (protein, go_id) row belongs to.
_GROUNDTRUTH_BUCKETS = ("nk", "lk", "pk", "known", "pk_known", "removed")

# ---------------------------------------------------------------------------
# All codes (GO + ECO) that are considered experimental
# ---------------------------------------------------------------------------
_EXP_CODES: list[str] = list(
    EXPERIMENTAL | {eco for eco, go in ECO_TO_CODE.items() if go in EXPERIMENTAL}
)

_NAMESPACES = ("F", "P", "C")

#: How the t1 side's truth is built, by the name a payload asks for it with.
#:
#: ``as_closed`` is what the platform has always done: a term the pivot cannot
#: express is dropped by the closure intersection, and the gain disappears
#: with it. It stays the default, so no stored window moves.
#:
#: ``projected`` adds the lab's rule (see :mod:`protea.core._truth_projection`):
#: a direct t1 term with no node at t0 is replaced by its first live ancestors
#: before the binding rule and the closure. On the 211 -> 227 window the two
#: profiles differ by 646 proteins and 2,610 entries, which is the whole of
#: the difference between PROTEA's truth and the lab's.
TRUTH_PROFILES: tuple[str, ...] = ("as_closed", "projected")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class EvalContext(NamedTuple):
    """The (old, new, snapshot) triple that identifies one evaluation delta.

    Bundles the three IDs that travel together through the metrics endpoints
    (``/scoring/prediction-sets/{id}/metrics``) and the
    :func:`compute_evaluation_data` / ``_reconciled`` helpers, keeping
    downstream signatures under the master-plan §3 6-param ceiling.
    """

    old_annotation_set_id: uuid.UUID
    new_annotation_set_id: uuid.UUID
    ontology_snapshot_id: uuid.UUID


class ReconciledWindow(NamedTuple):
    """The five ids a reconciled delta is built from, plus how to build it.

    One object for the same reason :class:`EvalContext` is one: the five
    travel together through every lookup, every refusal and every insert, and
    a signature that took them loose is a signature somebody can call with
    four. The profile joins them because it decides what the delta IS, not
    how it is stored: two profiles over the same five ids are two different
    truths, which is why it is part of the window's identity in the database
    too.
    """

    old_annotation_set_id: uuid.UUID
    new_annotation_set_id: uuid.UUID
    old_native_snapshot_id: uuid.UUID
    new_native_snapshot_id: uuid.UUID
    pivot_snapshot_id: uuid.UUID
    truth_profile: str = TRUTH_PROFILES[0]


@dataclass
class EvaluationData:
    """Computed ground-truth delta between two annotation sets."""

    # {protein_accession: {go_id}} — delta annotations per category
    nk: dict[str, set[str]] = field(default_factory=dict)
    lk: dict[str, set[str]] = field(default_factory=dict)
    pk: dict[str, set[str]] = field(default_factory=dict)
    # known-terms: ALL experimental annotations from OLD (for reference download)
    known: dict[str, set[str]] = field(default_factory=dict)
    # pk_known: old terms in PK namespaces only, passed as -known to cafaeval
    pk_known: dict[str, set[str]] = field(default_factory=dict)
    # removed: terms present at the window start and absent at its end.
    # Reported, never scored. See _classify_protein_deltas.
    removed: dict[str, set[str]] = field(default_factory=dict)
    #: What the projection did while this delta was built, or None under the
    #: default profile. BUILD TIME ONLY: the parquet holds the six buckets and
    #: nothing else, so a round trip returns None here. The operation stamps
    #: it on the ``evaluation_set`` row, which is where it has to survive.
    projection: dict[str, int] | None = None

    @property
    def nk_proteins(self) -> int:
        return len(self.nk)

    @property
    def lk_proteins(self) -> int:
        return len(self.lk)

    @property
    def pk_proteins(self) -> int:
        return len(self.pk)

    @property
    def nk_annotations(self) -> int:
        return sum(len(v) for v in self.nk.values())

    @property
    def lk_annotations(self) -> int:
        return sum(len(v) for v in self.lk.values())

    @property
    def pk_annotations(self) -> int:
        return sum(len(v) for v in self.pk.values())

    @property
    def known_terms_count(self) -> int:
        return sum(len(v) for v in self.known.values())

    @property
    def removed_proteins(self) -> int:
        return len(self.removed)

    @property
    def removed_annotations(self) -> int:
        return sum(len(v) for v in self.removed.values())

    @property
    def delta_proteins(self) -> int:
        return len(set(self.nk) | set(self.lk) | set(self.pk))

    def stats(self) -> dict:
        return {
            "delta_proteins": self.delta_proteins,
            "nk_proteins": self.nk_proteins,
            "lk_proteins": self.lk_proteins,
            "pk_proteins": self.pk_proteins,
            "nk_annotations": self.nk_annotations,
            "lk_annotations": self.lk_annotations,
            "pk_annotations": self.pk_annotations,
            "known_terms_count": self.known_terms_count,
            "removed_proteins": self.removed_proteins,
            "removed_annotations": self.removed_annotations,
            **(self.projection or {}),
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_go_maps(
    session: Session, snapshot_id: uuid.UUID
) -> tuple[dict[int, str], dict[int, str]]:
    """Load {go_term.id: go_id} and {go_term.id: aspect} for the snapshot.

    aspect is 'F' (molecular function), 'P' (biological process), or
    'C' (cellular component).
    """
    rows = session.execute(
        text("""
        SELECT id, go_id, aspect FROM go_term WHERE ontology_snapshot_id = :snap_id
    """),
        {"snap_id": snapshot_id},
    ).fetchall()
    id_map = {db_id: go_id for db_id, go_id, _ in rows}
    aspect_map = {db_id: aspect for db_id, _, aspect in rows if aspect}
    return id_map, aspect_map


def _build_negative_keys(
    session: Session,
    set_ids: list[uuid.UUID],
    children_map: dict[int, set[int]],
) -> set[tuple[str, int]]:
    """Build the set of (protein_accession, go_term_db_id) pairs to exclude.

    Collects NOT-qualified annotations from all given annotation sets and
    propagates them to all GO descendants via the DAG.
    """
    not_rows = session.execute(
        text("""
        SELECT DISTINCT protein_accession, go_term_id
        FROM protein_go_annotation
        WHERE annotation_set_id = ANY(:set_ids)
          AND qualifier LIKE '%NOT%'
    """),
        {"set_ids": set_ids},
    ).fetchall()

    negated_by_protein: dict[str, set[int]] = defaultdict(set)
    for protein_accession, go_term_id in not_rows:
        negated_by_protein[protein_accession].add(go_term_id)

    negative_keys: set[tuple[str, int]] = set()
    for protein_accession, term_ids in negated_by_protein.items():
        expanded: set[int] = set(term_ids)
        for tid in term_ids:
            expanded |= _get_descendants(tid, children_map)
        for tid in expanded:
            negative_keys.add((protein_accession, tid))

    return negative_keys


def _load_experimental_annotations_by_ns(
    session: Session,
    annotation_set_id: uuid.UUID,
    negative_keys: set[tuple[str, int]],
    go_id_map: dict[int, str],
    aspect_map: dict[int, str],
) -> dict[str, dict[str, set[str]]]:
    """Load experimental, non-negated annotations grouped by namespace.

    Returns {protein_accession: {aspect: {go_id}}} where aspect ∈ {'F', 'P', 'C'}.
    Terms without a known aspect are silently dropped.
    """
    rows = session.execute(
        text("""
        SELECT pga.protein_accession, pga.go_term_id
        FROM protein_go_annotation pga
        WHERE pga.annotation_set_id = :set_id
          AND pga.evidence_code = ANY(:exp_codes)
          AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
    """),
        {"set_id": annotation_set_id, "exp_codes": _EXP_CODES},
    ).fetchall()

    result: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for protein_accession, go_term_id in rows:
        if (protein_accession, go_term_id) in negative_keys:
            continue
        go_id = go_id_map.get(go_term_id)
        aspect = aspect_map.get(go_term_id)
        if go_id and aspect:
            result[protein_accession][aspect].add(go_id)
    return {p: dict(ns_terms) for p, ns_terms in result.items()}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _classify_protein_deltas(
    old_by_ns: dict[str, dict[str, set[str]]],
    new_by_ns: dict[str, dict[str, set[str]]],
) -> tuple[
    dict[str, set[str]],
    dict[str, set[str]],
    dict[str, set[str]],
    dict[str, set[str]],
    dict[str, set[str]],
]:
    """Sort each protein into the (NK, LK, PK, pk_known, removed) buckets.

    Per-(protein, namespace) classification following the CAFA5
    protocol; same protein can be LK in one namespace and PK in
    another. See :func:`compute_evaluation_data` for the full rules.

    ``removed`` holds terms present at the start of the window and absent at
    its end. It is reported rather than scored: the annotation corpus contracts
    as well as grows, and a window described only by its additions cannot say
    whether a drop in recall came from the method or from the corpus.
    """
    nk: dict[str, set[str]] = {}
    lk: dict[str, set[str]] = defaultdict(set)
    pk: dict[str, set[str]] = defaultdict(set)
    pk_known: dict[str, set[str]] = defaultdict(set)
    removed: dict[str, set[str]] = defaultdict(set)
    for protein in set(old_by_ns) | set(new_by_ns):
        old_ns_map = old_by_ns.get(protein, {})
        new_ns_map = new_by_ns.get(protein, {})
        # Removals are collected before the additions logic, because a protein
        # that keeps nothing at the end of the window exits early below. Those
        # are the proteins that lost the most, so computing removals after the
        # early exit would make the largest losses the only invisible ones.
        for ns in _NAMESPACES:
            gone_ns = old_ns_map.get(ns, set()) - new_ns_map.get(ns, set())
            if gone_ns:
                removed[protein] |= gone_ns
        new_all = {go for terms in new_ns_map.values() for go in terms}
        if not new_all:
            continue
        if not old_ns_map:
            # NK: no experimental annotations anywhere at t0.
            nk[protein] = new_all
            continue
        for ns in _NAMESPACES:
            old_ns = old_ns_map.get(ns, set())
            new_ns = new_ns_map.get(ns, set())
            delta_ns = new_ns - old_ns
            if not delta_ns:
                continue
            if not old_ns:
                lk[protein] |= delta_ns
            else:
                pk[protein] |= delta_ns
                pk_known[protein] |= old_ns
    return nk, dict(lk), dict(pk), dict(pk_known), dict(removed)


def compute_evaluation_data(
    session: Session,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
) -> EvaluationData:
    """Compute NK/LK/PK ground truth following the CAFA5 protocol.

    Classification is per ``(protein, namespace)``:

    - **NK** — protein had no experimental annotations in any namespace at
      ``t0``.
    - **LK** — protein had annotations in some namespaces at ``t0``, but not
      in namespace ``S``; gained new terms in ``S`` → those terms are LK
      ground truth.
    - **PK** — protein had annotations in namespace ``S`` at ``t0`` and
      gained new terms in ``S`` → those novel terms are PK ground truth;
      old terms in ``S`` are stored in ``pk_known`` for the cafaeval
      ``-known`` flag.

    The same protein can be simultaneously LK in one namespace and PK in
    another.
    """
    go_id_map, aspect_map = _load_go_maps(session, ontology_snapshot_id)
    children_map = _load_children_map(session, ontology_snapshot_id)
    negative_keys = _build_negative_keys(
        session,
        [old_annotation_set_id, new_annotation_set_id],
        children_map,
    )
    old_by_ns = _load_experimental_annotations_by_ns(
        session, old_annotation_set_id, negative_keys, go_id_map, aspect_map
    )
    new_by_ns = _load_experimental_annotations_by_ns(
        session, new_annotation_set_id, negative_keys, go_id_map, aspect_map
    )
    nk, lk, pk, pk_known, removed = _classify_protein_deltas(old_by_ns, new_by_ns)
    known = {
        p: {go for terms in ns_map.values() for go in terms} for p, ns_map in old_by_ns.items()
    }
    return EvaluationData(nk=nk, lk=lk, pk=pk, pk_known=pk_known, known=known, removed=removed)


# ---------------------------------------------------------------------------
# Cross-OBO reconciliation (CAFA protocol for mismatched snapshots)
# ---------------------------------------------------------------------------


def _load_experimental_raw_go_ids(
    session: Session, annotation_set_id: uuid.UUID
) -> dict[str, set[str]]:
    """Return ``{protein: {native_go_id}}`` for experimental non-NOT rows."""
    rows = session.execute(
        text("""
        SELECT pga.protein_accession, gt.go_id
        FROM protein_go_annotation pga
        JOIN go_term gt ON gt.id = pga.go_term_id
        WHERE pga.annotation_set_id = :set_id
          AND pga.evidence_code = ANY(:exp_codes)
          AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
    """),
        {"set_id": annotation_set_id, "exp_codes": _EXP_CODES},
    ).fetchall()
    out: dict[str, set[str]] = defaultdict(set)
    for prot, go_id in rows:
        out[prot].add(go_id)
    return dict(out)


def _load_not_raw_go_ids(session: Session, annotation_set_id: uuid.UUID) -> dict[str, set[str]]:
    """Return ``{protein: {native_go_id}}`` for NOT-qualified rows."""
    rows = session.execute(
        text("""
        SELECT DISTINCT pga.protein_accession, gt.go_id
        FROM protein_go_annotation pga
        JOIN go_term gt ON gt.id = pga.go_term_id
        WHERE pga.annotation_set_id = :set_id
          AND pga.qualifier LIKE '%NOT%'
    """),
        {"set_id": annotation_set_id},
    ).fetchall()
    out: dict[str, set[str]] = defaultdict(set)
    for prot, go_id in rows:
        out[prot].add(go_id)
    return dict(out)


def _drop_binding_only(
    session: Session, snapshot_id: uuid.UUID, raw: dict[str, set[str]]
) -> int:
    """Apply the protein-binding rule to one side's DIRECT annotations.

    The binding subtree is resolved per snapshot rather than hardcoded, because
    it is not a constant: 920 terms under the snapshot behind GOA 220, and the
    ontology prunes and grows across the series this campaign spans.
    """
    binding = _bfs_closure(
        {PROTEIN_BINDING}, _load_children_by_go_id(session, snapshot_id)
    )
    _, aspect_by_go_id = _load_pivot_term_universe(session, snapshot_id)
    return drop_binding_only_mfo(raw, binding, aspect_by_go_id)


def _reconcile_experimental_side(
    session: Session,
    annotation_set_id: uuid.UUID,
    native_snapshot_id: uuid.UUID,
    pivot_go_ids: set[str],
    pivot_aspect: dict[str, str],
    project_onto: set[str] | None = None,
) -> tuple[dict[str, dict[str, set[str]]], Projection | None]:
    """CAFA steps 1–2 for experimental positives on one side.

    Per protein: propagate ancestors under the *native* DAG (True Path Rule),
    intersect with the pivot term universe, then bucket by the pivot aspect.
    Step 3 (re-propagate under pivot) is deferred to cafaeval, which applies
    ancestor propagation before scoring and produces the same closure.
    """
    native_parents = _load_parents_by_go_id(session, native_snapshot_id)
    raw = _load_experimental_raw_go_ids(session, annotation_set_id)
    projection = None
    if project_onto is not None:
        # Before the binding rule and before any propagation: the rule is
        # about DIRECT annotations, and after either step the substitution
        # would mean something else.
        native_live, native_aspect = _live_terms_with_aspect(session, native_snapshot_id)
        projection = project_direct_annotations(
            raw,
            Graph(
                parents=native_parents,
                aspect=native_aspect,
                live=native_live,
                binding=frozenset(
                    _bfs_closure(
                        {PROTEIN_BINDING}, _load_children_by_go_id(session, native_snapshot_id)
                    )
                ),
            ),
            project_onto,
        )
    _drop_binding_only(session, native_snapshot_id, raw)

    out: dict[str, dict[str, set[str]]] = {}
    for protein, go_ids in raw.items():
        closure = _bfs_closure(go_ids, native_parents)
        in_pivot = closure & pivot_go_ids
        if not in_pivot:
            continue
        ns_map: dict[str, set[str]] = defaultdict(set)
        for go_id in in_pivot:
            aspect = pivot_aspect.get(go_id)
            if aspect:
                ns_map[aspect].add(go_id)
        if ns_map:
            out[protein] = {ns: terms for ns, terms in ns_map.items()}
    return out, projection


def _reconcile_not_side(
    session: Session,
    annotation_set_id: uuid.UUID,
    native_snapshot_id: uuid.UUID,
    pivot_go_ids: set[str],
    pivot_children: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Reconcile NOT-qualified terms to pivot via True Path Rule contrapositive.

    Per protein: propagate descendants under the native DAG (matching the
    same-snapshot path), intersect with pivot, then propagate descendants under
    the pivot DAG to capture subtypes only the pivot ontology sees. The final
    set is the pivot go_ids to exclude from the experimental closure.

    This preserves PROTEA's NOT propagation semantics — democafa just drops
    NOT rows — while still producing a pivot-consistent exclusion set.
    """
    native_children = _load_children_by_go_id(session, native_snapshot_id)
    raw = _load_not_raw_go_ids(session, annotation_set_id)

    out: dict[str, set[str]] = {}
    for protein, go_ids in raw.items():
        closure_native = _bfs_closure(go_ids, native_children)
        in_pivot = closure_native & pivot_go_ids
        if not in_pivot:
            continue
        closure_pivot = _bfs_closure(in_pivot, pivot_children)
        out[protein] = closure_pivot & pivot_go_ids
    return out


def _apply_negatives(
    experimental: dict[str, dict[str, set[str]]],
    negatives: dict[str, set[str]],
) -> dict[str, dict[str, set[str]]]:
    """Remove negated go_ids from each namespace bucket. Drops empty entries."""
    cleaned: dict[str, dict[str, set[str]]] = {}
    for protein, ns_map in experimental.items():
        neg = negatives.get(protein)
        if not neg:
            cleaned[protein] = {ns: set(terms) for ns, terms in ns_map.items()}
            continue
        new_ns_map = {ns: terms - neg for ns, terms in ns_map.items()}
        new_ns_map = {ns: terms for ns, terms in new_ns_map.items() if terms}
        if new_ns_map:
            cleaned[protein] = new_ns_map
    return cleaned


def _reconcile_both_sides(
    session: Session,
    window: ReconciledWindow,
    pivot_go_ids: set[str],
    pivot_aspect: dict[str, str],
    pivot_children: dict[str, set[str]],
) -> tuple[
    dict[str, dict[str, set[str]]],
    dict[str, dict[str, set[str]]],
    dict[str, set[str]],
    Projection | None,
]:
    """Both sides reconciled, and the NOT terms unioned across them.

    ONLY THE t1 SIDE PROJECTS, and only onto what is LIVE at t0. The target is
    the old side's own graph rather than the pivot's: the pivot keeps obsolete
    terms, and landing the truth on one would put it on a node cafaeval's
    graph does not have.

    The NOT union is applied to both sides, which is the same thing the
    same-snapshot path does: a term a protein is known NOT to have at either
    end is not a gain at the other.
    """
    project_onto = None
    if window.truth_profile == "projected":
        project_onto, _ = _live_terms_with_aspect(session, window.old_native_snapshot_id)

    old_exp, _ = _reconcile_experimental_side(
        session,
        window.old_annotation_set_id,
        window.old_native_snapshot_id,
        pivot_go_ids,
        pivot_aspect,
    )
    new_exp, projection = _reconcile_experimental_side(
        session,
        window.new_annotation_set_id,
        window.new_native_snapshot_id,
        pivot_go_ids,
        pivot_aspect,
        project_onto,
    )
    merged_neg: dict[str, set[str]] = defaultdict(set)
    for side, native in (
        (window.old_annotation_set_id, window.old_native_snapshot_id),
        (window.new_annotation_set_id, window.new_native_snapshot_id),
    ):
        for protein, terms in _reconcile_not_side(
            session, side, native, pivot_go_ids, pivot_children
        ).items():
            merged_neg[protein] |= terms
    return old_exp, new_exp, merged_neg, projection


def compute_evaluation_data_reconciled(
    session: Session, window: ReconciledWindow
) -> EvaluationData:
    """CAFA-compliant evaluation delta across mismatched ontology snapshots.

    Applies the democafa ``filter_terms_given_obo`` protocol per side:

      1. Load experimental annotations under each set's native snapshot.
      2. Propagate ancestors under the *native* DAG (True Path Rule).
      3. Intersect with the pivot snapshot's term universe.

    Step 4 (re-propagate under pivot) is handled by cafaeval downstream, which
    applies ancestor propagation before scoring — ``prop(prop(x)) == prop(x)``.

    NOT-qualifier exclusion preserves PROTEA's True Path Rule contrapositive:
    NOT terms are propagated to descendants under the native DAG, intersected
    with pivot, then further propagated under the pivot DAG. The union across
    both annotation sets is applied to both sides, matching same-snapshot
    behaviour.
    """
    if window.truth_profile not in TRUTH_PROFILES:
        raise ValueError(
            f"unknown truth_profile {window.truth_profile!r}; expected one of "
            f"{TRUTH_PROFILES}"
        )
    pivot_go_ids, pivot_aspect = _load_pivot_term_universe(session, window.pivot_snapshot_id)
    pivot_children = _load_children_by_go_id(session, window.pivot_snapshot_id)

    # Only the t1 side projects, and only onto what is LIVE at t0. The target
    # is the old side's own graph rather than the pivot's: the pivot keeps
    # obsolete terms, and landing on one would put the truth on a node
    # cafaeval's graph does not have.
    old_exp, new_exp, merged_neg, projection = _reconcile_both_sides(
        session, window, pivot_go_ids, pivot_aspect, pivot_children
    )

    old_by_ns = _apply_negatives(old_exp, merged_neg)
    new_by_ns = _apply_negatives(new_exp, merged_neg)
    nk, lk, pk, pk_known, removed = _classify_protein_deltas(old_by_ns, new_by_ns)
    known = {
        p: {go for terms in ns_map.values() for go in terms} for p, ns_map in old_by_ns.items()
    }
    return EvaluationData(
        nk=nk,
        lk=lk,
        pk=pk,
        pk_known=pk_known,
        known=known,
        removed=removed,
        projection=projection.as_dict() if projection else None,
    )


# The parquet round trip and the artifact-store reuse rule live in a sibling,
# and are re-exported here because the names are the public surface and every
# caller imports them from this module.
from protea.core._evaluation_groundtruth import (  # noqa: E402
    deserialize_evaluation_data_from_bytes,
    groundtruth_key_for,
    load_evaluation_data_for_set,
    serialize_evaluation_data_to_parquet,
)

__all__ = [
    "EvalContext",
    "EvaluationData",
    "compute_evaluation_data",
    "ReconciledWindow",
    "TRUTH_PROFILES",
    "compute_evaluation_data_reconciled",
    "deserialize_evaluation_data_from_bytes",
    "groundtruth_key_for",
    "load_evaluation_data_for_set",
    "serialize_evaluation_data_to_parquet",
]
