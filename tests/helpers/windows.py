"""A minimal evaluation window, built through the ORM so defaults apply."""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session


def make_window(session: Session, *, snapshot_version: str = "window-test") -> uuid.UUID:
    """An evaluation set over two annotation sets of one snapshot, 211 to 227."""
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

    snapshot = OntologySnapshot(obo_url="file:///none", obo_version=snapshot_version)
    session.add(snapshot)
    session.flush()
    old = AnnotationSet(source="goa", source_version="211", ontology_snapshot_id=snapshot.id)
    new = AnnotationSet(source="goa", source_version="227", ontology_snapshot_id=snapshot.id)
    session.add_all([old, new])
    session.flush()
    window = EvaluationSet(
        old_annotation_set_id=old.id,
        new_annotation_set_id=new.id,
        pivot_snapshot_id=snapshot.id,
        old_native_snapshot_id=snapshot.id,
        new_native_snapshot_id=snapshot.id,
        stats={},
    )
    session.add(window)
    session.flush()
    return window.id
