"""Choosing the delta builder that can actually resolve two annotation sets.

WHY THIS MODULE EXISTS.

:func:`protea.core.evaluation.compute_evaluation_data` builds ONE ``go_id`` map
from ONE ontology snapshot and resolves BOTH annotation sets through it.
``go_term.id`` is a surrogate scoped to a snapshot, and in this database the id
spaces of different snapshots are DISJOINT rather than overlapping. Of the six
pairs among the 220, 226, 227 and 230 annotation sets, exactly one shares a
single internal id, and that pair is the two sets that happen to sit on the same
snapshot. Neither frame the campaign uses is that pair.

So calling it across a temporal window does not degrade, it fails completely,
and it fails in silence: unresolvable ids are dropped where
``_load_annotations_by_aspect`` tests ``if go_id and aspect``. Measured against
the live database on the GOA 226 to 227 window:

    caller passes the OLD snapshot     NK=0       LK=0    PK=0
    caller passes the NEW snapshot     NK=88,193  LK=0    PK=0
    reconciled path                    NK=523     LK=622  PK=5,672

The second is the dangerous one. Nothing raises, every protein is classified
NK, and a request for one category is answered over the whole annotation set:
88,193 where the truth is 523. It returns a number that looks like a result
rather than a zero that looks like an absence.

``generate_evaluation_set`` has always branched on this correctly, which is why
the published board is sound. Four other call sites did not, one of them
reachable as ``GET /prediction-sets/{id}/metrics`` with all three ids as free
query parameters, and its router docstring asked the CALLER to align them "for
valid scoring". A contract addressed to the caller in prose is the defect this
module closes: the branch is made here, once, and a triple that cannot be
resolved is refused by name instead of quietly returning the wrong population.
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session


class SnapshotMismatchError(ValueError):
    """A pivot snapshot was named that cannot resolve the annotation sets given."""


def compute_evaluation_data_for_sets(
    session: Session,
    old_annotation_set_id: uuid.UUID,
    new_annotation_set_id: uuid.UUID,
    pivot_snapshot_id: uuid.UUID,
):
    """The evaluation delta for two annotation sets, on whichever path is correct.

    Each set's native snapshot is read from the set itself, which is
    authoritative. ``pivot_snapshot_id`` is the term universe to express the
    answer in, and is NOT evidence about where either set lives. Same-snapshot
    work keeps the direct path; anything else takes the reconciled one.

    See the module docstring for what goes wrong when this branch is skipped.

    Raises
    ------
    SnapshotMismatchError
        An annotation set or the pivot snapshot does not exist.
    """
    from protea.core.evaluation import (
        compute_evaluation_data,
        compute_evaluation_data_reconciled,
    )
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )

    old_set = session.get(AnnotationSet, old_annotation_set_id)
    if old_set is None:
        raise SnapshotMismatchError(f"AnnotationSet {old_annotation_set_id} not found")
    new_set = session.get(AnnotationSet, new_annotation_set_id)
    if new_set is None:
        raise SnapshotMismatchError(f"AnnotationSet {new_annotation_set_id} not found")
    if session.get(OntologySnapshot, pivot_snapshot_id) is None:
        raise SnapshotMismatchError(
            f"OntologySnapshot {pivot_snapshot_id} not found, so it cannot be the "
            "term universe for this delta"
        )

    old_native = old_set.ontology_snapshot_id
    new_native = new_set.ontology_snapshot_id
    if old_native == new_native == pivot_snapshot_id:
        return compute_evaluation_data(
            session,
            old_annotation_set_id=old_annotation_set_id,
            new_annotation_set_id=new_annotation_set_id,
            ontology_snapshot_id=pivot_snapshot_id,
        )
    return compute_evaluation_data_reconciled(
        session,
        old_annotation_set_id,
        new_annotation_set_id,
        old_native,
        new_native,
        pivot_snapshot_id,
    )
