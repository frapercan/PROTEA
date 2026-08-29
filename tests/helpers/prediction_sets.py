"""Minimal prediction sets, built through the ORM so defaults apply.

Shared by the tests that exercise what a stored set says about itself: the
batch revision guard and the three surfaces that name an arm. Both need a real
row rather than a dict, because the thing under test in each is a SQL read of
``meta`` against the column the fleet will meet.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session


def make_parents(session: Session) -> dict[str, uuid.UUID]:
    """The three rows a prediction set cannot exist without."""
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )
    from protea.infrastructure.orm.models.embedding.embedding_config import (
        EmbeddingConfig,
    )

    snapshot = OntologySnapshot(obo_url="file:///none", obo_version="test")
    embedding = EmbeddingConfig(model_name="test", layer_indices=[])
    session.add_all([snapshot, embedding])
    session.flush()
    annotation = AnnotationSet(source="test", ontology_snapshot_id=snapshot.id)
    session.add(annotation)
    session.flush()
    return {
        "snapshot": snapshot.id,
        "annotation": annotation.id,
        "embedding": embedding.id,
    }


def make_prediction_set(
    session: Session, parents: dict[str, uuid.UUID], meta: dict[str, Any]
) -> uuid.UUID:
    """A prediction set carrying exactly the ``meta`` given."""
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

    row = PredictionSet(
        embedding_config_id=parents["embedding"],
        annotation_set_id=parents["annotation"],
        ontology_snapshot_id=parents["snapshot"],
        limit_per_entry=30,
        meta=meta,
    )
    session.add(row)
    session.flush()
    return row.id
