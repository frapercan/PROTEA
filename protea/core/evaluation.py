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

Output format (matching CAFA evaluator): 2-column TSV, no header.
  protein_accession \\t go_id
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL

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

@dataclass
class EvaluationData:
    """Computed ground-truth delta between two annotation sets."""

    # {protein_accession: {go_id}} — delta annotations per category
    nk: dict[str, set[str]] = field(default_factory=dict)
    lk: dict[str, set[str]] = field(default_factory=dict)
    pk: dict[str, set[str]] = field(default_factory=dict)
    # known-terms: ALL experimental annotations from OLD (for reference download)
    known: dict[str, set[str]] = field(default_factory=dict)
    # pk_known: old terms in PK namespaces only — passed as -known to cafaeval
    pk_known: dict[str, set[str]] = field(default_factory=dict)

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
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_children_map(session: Session, snapshot_id: uuid.UUID) -> dict[int, set[int]]:
    """Load GO DAG as {parent_go_term_id: {child_go_term_id}} for a snapshot.

    Only is_a and part_of relationships are used for NOT-propagation, matching
    the CAFA evaluation protocol.
    """
    rows = session.execute(text("""
        SELECT parent_go_term_id, child_go_term_id
        FROM go_term_relationship
        WHERE ontology_snapshot_id = :snap_id
          AND relation_type IN ('is_a', 'part_of')
    """), {"snap_id": snapshot_id}).fetchall()

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
    rows = session.execute(text("""
        SELECT id, go_id, aspect FROM go_term WHERE ontology_snapshot_id = :snap_id
    """), {"snap_id": snapshot_id}).fetchall()
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
    not_rows = session.execute(text("""
        SELECT DISTINCT protein_accession, go_term_id
        FROM protein_go_annotation
        WHERE annotation_set_id = ANY(:set_ids)
          AND qualifier LIKE '%NOT%'
    """), {"set_ids": set_ids}).fetchall()

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
    rows = session.execute(text("""
        SELECT pga.protein_accession, pga.go_term_id
        FROM protein_go_annotation pga
        WHERE pga.annotation_set_id = :set_id
          AND pga.evidence_code = ANY(:exp_codes)
          AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
    """), {"set_id": annotation_set_id, "exp_codes": _EXP_CODES}).fetchall()

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

def compute_evaluation_data(
    session: Session,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
) -> EvaluationData:
    """Compute NK/LK/PK ground truth following the CAFA5 protocol.

    Classification is per (protein, namespace):

      NK  — protein had no experimental annotations in any namespace at t0.
      LK  — protein had annotations in some namespaces at t0, but not in
             namespace S; gained new terms in S → those terms are LK ground truth.
      PK  — protein had annotations in namespace S at t0 and gained new terms
             in S → those novel terms are PK ground truth; old terms in S are
             stored in ``pk_known`` for the cafaeval ``-known`` flag.

    The same protein can be simultaneously LK in one namespace and PK in another.
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

    nk: dict[str, set[str]] = {}
    lk: dict[str, set[str]] = defaultdict(set)
    pk: dict[str, set[str]] = defaultdict(set)
    pk_known: dict[str, set[str]] = defaultdict(set)

    all_proteins = set(old_by_ns) | set(new_by_ns)
    for protein in all_proteins:
        old_ns_map = old_by_ns.get(protein, {})
        new_ns_map = new_by_ns.get(protein, {})

        new_all = {go for terms in new_ns_map.values() for go in terms}
        if not new_all:
            continue

        had_anything_old = bool(old_ns_map)

        if not had_anything_old:
            # NK: no experimental annotations anywhere at t0.
            # Novel = all new terms (nothing to subtract).
            nk[protein] = new_all
        else:
            # Classify per namespace.
            for ns in _NAMESPACES:
                old_ns = old_ns_map.get(ns, set())
                new_ns = new_ns_map.get(ns, set())
                delta_ns = new_ns - old_ns
                if not delta_ns:
                    continue
                if not old_ns:
                    # LK: protein had nothing in this namespace at t0.
                    lk[protein] |= delta_ns
                else:
                    # PK: protein had annotations in this namespace at t0.
                    pk[protein] |= delta_ns
                    pk_known[protein] |= old_ns

    # known = all old experimental annotations flattened (for reference download)
    known = {
        p: {go for terms in ns_map.values() for go in terms}
        for p, ns_map in old_by_ns.items()
    }

    return EvaluationData(
        nk=nk,
        lk=dict(lk),
        pk=dict(pk),
        pk_known=dict(pk_known),
        known=known,
    )
