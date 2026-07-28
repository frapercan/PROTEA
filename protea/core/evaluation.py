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

import io
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

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
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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
    return EvaluationData(
        nk=nk, lk=lk, pk=pk, pk_known=pk_known, known=known, removed=removed
    )


# ---------------------------------------------------------------------------
# Cross-OBO reconciliation (CAFA protocol for mismatched snapshots)
# ---------------------------------------------------------------------------


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


def _load_not_raw_go_ids(
    session: Session, annotation_set_id: uuid.UUID
) -> dict[str, set[str]]:
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


def _reconcile_experimental_side(
    session: Session,
    annotation_set_id: uuid.UUID,
    native_snapshot_id: uuid.UUID,
    pivot_go_ids: set[str],
    pivot_aspect: dict[str, str],
) -> dict[str, dict[str, set[str]]]:
    """CAFA steps 1–2 for experimental positives on one side.

    Per protein: propagate ancestors under the *native* DAG (True Path Rule),
    intersect with the pivot term universe, then bucket by the pivot aspect.
    Step 3 (re-propagate under pivot) is deferred to cafaeval, which applies
    ancestor propagation before scoring and produces the same closure.
    """
    native_parents = _load_parents_by_go_id(session, native_snapshot_id)
    raw = _load_experimental_raw_go_ids(session, annotation_set_id)

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
    return out


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


def compute_evaluation_data_reconciled(
    session: Session,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    old_native_snapshot_id: uuid.UUID,
    new_native_snapshot_id: uuid.UUID,
    pivot_snapshot_id: uuid.UUID,
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
    pivot_go_ids, pivot_aspect = _load_pivot_term_universe(session, pivot_snapshot_id)
    pivot_children = _load_children_by_go_id(session, pivot_snapshot_id)

    old_exp = _reconcile_experimental_side(
        session, old_annotation_set_id, old_native_snapshot_id, pivot_go_ids, pivot_aspect
    )
    new_exp = _reconcile_experimental_side(
        session, new_annotation_set_id, new_native_snapshot_id, pivot_go_ids, pivot_aspect
    )

    old_neg = _reconcile_not_side(
        session, old_annotation_set_id, old_native_snapshot_id, pivot_go_ids, pivot_children
    )
    new_neg = _reconcile_not_side(
        session, new_annotation_set_id, new_native_snapshot_id, pivot_go_ids, pivot_children
    )
    merged_neg: dict[str, set[str]] = defaultdict(set)
    for src in (old_neg, new_neg):
        for protein, terms in src.items():
            merged_neg[protein] |= terms

    old_by_ns = _apply_negatives(old_exp, merged_neg)
    new_by_ns = _apply_negatives(new_exp, merged_neg)
    nk, lk, pk, pk_known, removed = _classify_protein_deltas(old_by_ns, new_by_ns)
    known = {
        p: {go for terms in ns_map.values() for go in terms} for p, ns_map in old_by_ns.items()
    }
    return EvaluationData(
        nk=nk, lk=lk, pk=pk, pk_known=pk_known, known=known, removed=removed
    )


def _eval_data_to_dataframe(data: EvaluationData):
    """Flatten EvaluationData's five buckets into a long DataFrame.

    Columns: ``protein_accession`` (str), ``go_id`` (str), ``bucket`` (categorical
    one of nk/lk/pk/known/pk_known). One row per (protein, go_id, bucket) triple.
    """
    import pandas as pd

    rows: list[tuple[str, str, str]] = []
    for bucket_name, bucket_dict in (
        ("nk", data.nk),
        ("lk", data.lk),
        ("pk", data.pk),
        ("known", data.known),
        ("pk_known", data.pk_known),
        ("removed", data.removed),
    ):
        for protein, go_ids in bucket_dict.items():
            for go_id in go_ids:
                rows.append((protein, go_id, bucket_name))
    df = pd.DataFrame(rows, columns=["protein_accession", "go_id", "bucket"])
    df["bucket"] = df["bucket"].astype(
        pd.CategoricalDtype(categories=list(_GROUNDTRUTH_BUCKETS))
    )
    return df


def _dataframe_to_eval_data(df) -> EvaluationData:
    """Inverse of ``_eval_data_to_dataframe``."""
    nk: dict[str, set[str]] = defaultdict(set)
    lk: dict[str, set[str]] = defaultdict(set)
    pk: dict[str, set[str]] = defaultdict(set)
    known: dict[str, set[str]] = defaultdict(set)
    pk_known: dict[str, set[str]] = defaultdict(set)
    removed: dict[str, set[str]] = defaultdict(set)
    # Files written before the removed bucket existed simply carry no such
    # rows, so an older artifact deserializes with removed empty rather than
    # failing.
    bucket_to_dict = {
        "nk": nk, "lk": lk, "pk": pk, "known": known,
        "pk_known": pk_known, "removed": removed,
    }
    for protein, go_id, bucket in df[["protein_accession", "go_id", "bucket"]].itertuples(
        index=False, name=None
    ):
        bucket_to_dict[str(bucket)][str(protein)].add(str(go_id))
    return EvaluationData(
        nk=dict(nk), lk=dict(lk), pk=dict(pk),
        known=dict(known), pk_known=dict(pk_known), removed=dict(removed),
    )


def serialize_evaluation_data_to_parquet(data: EvaluationData, dest: Path) -> Path:
    """Write ``data`` to a parquet file at ``dest`` (creates parent dirs).

    Returns ``dest`` for convenience. Uses snappy compression and the long
    layout produced by ``_eval_data_to_dataframe`` — kept stable since
    consumers parse it back with ``deserialize_evaluation_data_from_bytes``.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    df = _eval_data_to_dataframe(data)
    df.to_parquet(dest, index=False, compression="snappy")
    return dest


def deserialize_evaluation_data_from_bytes(blob: bytes) -> EvaluationData:
    """Parse parquet bytes (as returned by ``ArtifactStore.get``) into EvaluationData."""
    import pandas as pd

    df = pd.read_parquet(io.BytesIO(blob))
    return _dataframe_to_eval_data(df)


def load_evaluation_data_for_set(session: Session, eval_set) -> tuple[EvaluationData, uuid.UUID]:
    """Load ground-truth for an EvaluationSet row.

    Strict reuse path: if ``eval_set.groundtruth_uri`` is set, deserializes the
    persisted parquet via the configured ArtifactStore and returns it. If not
    set, raises — recomputation on-the-fly is intentionally not allowed (see
    the project's "no on-the-fly reuse" rule). Use
    ``scripts/backfill_evaluation_groundtruth.py`` to materialize artifacts for
    legacy EvaluationSet rows that predate this column.

    Returns the EvaluationData plus the pivot OntologySnapshot ID — the caller
    should use the pivot snapshot (not the old set's) when loading the OBO for
    cafaeval, since propagated go_ids live in pivot term space.
    """
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    ann_old = session.get(AnnotationSet, eval_set.old_annotation_set_id)
    ann_new = session.get(AnnotationSet, eval_set.new_annotation_set_id)

    stats = eval_set.stats or {}
    pivot_raw = stats.get("pivot_ontology_snapshot_id")
    if pivot_raw:
        pivot_id = uuid.UUID(str(pivot_raw))
    else:
        # Both ann_new and ann_old are validated non-None by the caller
        # (run_cafa_evaluation); the ternary short-circuits before
        # dereferencing ann_old when ann_new is set.
        pivot_id = ann_new.ontology_snapshot_id if ann_new else ann_old.ontology_snapshot_id  # type: ignore[union-attr]

    if not eval_set.groundtruth_uri:
        raise RuntimeError(
            f"EvaluationSet {eval_set.id} has no groundtruth_uri. "
            "Run scripts/backfill_evaluation_groundtruth.py to materialize "
            "the parquet artifact, or regenerate via /annotations/evaluation-sets/generate."
        )

    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings(project_root)
    store = get_artifact_store(settings)
    key = groundtruth_key_for(eval_set.id)
    blob = store.get(key)
    data = deserialize_evaluation_data_from_bytes(blob)
    return data, pivot_id


def groundtruth_key_for(eval_set_id) -> str:
    """Storage key under which an EvaluationSet's ground-truth parquet lives."""
    return f"eval_groundtruth/{eval_set_id}/groundtruth.parquet"
