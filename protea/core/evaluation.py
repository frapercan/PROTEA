"""CAFA-style evaluation data computation.

This module computes the ground-truth delta between two AnnotationSets
(old → new) following the CAFA evaluation protocol:

  1. Experimental evidence codes only (EXP, IDA, IMP, …)
  2. NOT-qualifier annotations are excluded — including their GO descendants
     propagated transitively through the is_a / part_of DAG.
  3. Delta proteins = proteins that gained ≥ 1 new (protein, go_term) pair.
  4. NK  = delta proteins with ZERO experimental annotations in OLD.
  5. LK  = delta proteins with ≥ 1 experimental annotation in OLD.
  6. PK  = same annotation set as LK; use with known-terms for evaluation.
  7. known-terms = ALL experimental annotations from OLD (not delta-filtered).

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


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class EvaluationData:
    """Computed ground-truth delta between two annotation sets."""

    # {protein_accession: {go_id}} — delta annotations per category
    nk: dict[str, set[str]] = field(default_factory=dict)
    lk: dict[str, set[str]] = field(default_factory=dict)
    # known-terms: ALL experimental annotations from OLD (all proteins)
    known: dict[str, set[str]] = field(default_factory=dict)

    @property
    def nk_proteins(self) -> int:
        return len(self.nk)

    @property
    def lk_proteins(self) -> int:
        return len(self.lk)

    @property
    def nk_annotations(self) -> int:
        return sum(len(v) for v in self.nk.values())

    @property
    def lk_annotations(self) -> int:
        return sum(len(v) for v in self.lk.values())

    @property
    def known_terms_count(self) -> int:
        return sum(len(v) for v in self.known.values())

    @property
    def delta_proteins(self) -> int:
        return self.nk_proteins + self.lk_proteins

    def stats(self) -> dict:
        return {
            "delta_proteins": self.delta_proteins,
            "nk_proteins": self.nk_proteins,
            "lk_proteins": self.lk_proteins,
            "nk_annotations": self.nk_annotations,
            "lk_annotations": self.lk_annotations,
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


def _load_experimental_annotations(
    session: Session,
    annotation_set_id: uuid.UUID,
    negative_keys: set[tuple[str, int]],
    go_id_map: dict[int, str],
) -> dict[str, set[str]]:
    """Load all experimental, non-negated annotations from an annotation set.

    Returns {protein_accession: {go_id}}.
    negative_keys contains (protein_accession, go_term_db_id) pairs to exclude.
    """
    rows = session.execute(text("""
        SELECT pga.protein_accession, pga.go_term_id
        FROM protein_go_annotation pga
        WHERE pga.annotation_set_id = :set_id
          AND pga.evidence_code = ANY(:exp_codes)
          AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
    """), {"set_id": annotation_set_id, "exp_codes": _EXP_CODES}).fetchall()

    result: dict[str, set[str]] = defaultdict(set)
    for protein_accession, go_term_id in rows:
        if (protein_accession, go_term_id) in negative_keys:
            continue
        go_id = go_id_map.get(go_term_id)
        if go_id:
            result[protein_accession].add(go_id)
    return dict(result)


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

    # Group negated terms by protein, expand to descendants
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


def _load_go_id_map(session: Session, snapshot_id: uuid.UUID) -> dict[int, str]:
    """Load {go_term.id: go_term.go_id} for the snapshot."""
    rows = session.execute(text("""
        SELECT id, go_id FROM go_term WHERE ontology_snapshot_id = :snap_id
    """), {"snap_id": snapshot_id}).fetchall()
    return {db_id: go_id for db_id, go_id in rows}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_evaluation_data(
    session: Session,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
) -> EvaluationData:
    """Compute NK/LK ground truth and known-terms from two annotation sets.

    This is the main entry point used both by the generation operation
    (to persist stats) and by the download endpoints (to stream TSV data).

    Steps:
      1. Load GO DAG children map for NOT propagation.
      2. Build negative_keys from NOT annotations in both sets.
      3. Load experimental annotations from OLD and NEW (excluding negatives).
      4. Compute delta = new - old per protein.
      5. Classify delta proteins into NK / LK.
      6. Collect known-terms from OLD (all proteins, no delta filter).
    """
    go_id_map = _load_go_id_map(session, ontology_snapshot_id)
    children_map = _load_children_map(session, ontology_snapshot_id)

    negative_keys = _build_negative_keys(
        session,
        [old_annotation_set_id, new_annotation_set_id],
        children_map,
    )

    old_terms = _load_experimental_annotations(
        session, old_annotation_set_id, negative_keys, go_id_map
    )
    new_terms = _load_experimental_annotations(
        session, new_annotation_set_id, negative_keys, go_id_map
    )

    nk: dict[str, set[str]] = {}
    lk: dict[str, set[str]] = {}

    all_proteins = set(old_terms) | set(new_terms)
    for protein in all_proteins:
        old_set = old_terms.get(protein, set())
        new_set = new_terms.get(protein, set())

        if not new_set:
            continue

        novel = new_set - old_set
        if not novel:
            continue

        if not old_set:
            nk[protein] = novel
        else:
            lk[protein] = novel

    return EvaluationData(nk=nk, lk=lk, known=old_terms)
