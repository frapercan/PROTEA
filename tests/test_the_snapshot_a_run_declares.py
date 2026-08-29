"""A run cannot declare an ontology snapshot its terms do not come from.

WHY THIS TEST EXISTS. All three prediction sets in the store declare snapshot
a24e7d91 and hold zero terms belonging to it: every term resolves to 36038118,
which is the snapshot their annotation set draws from. The payload's value was
checked for existence, recorded where a reader will trust it, and never
compared against anything. That is the defect class this project keeps meeting,
and the reason it survived is that nothing downstream read the field on the
paths those runs took.

It is read on other paths. ``_lineage_feature`` builds its parent map and
``_ia_feature`` its information-accretion map from the declared snapshot, so a
mismatched run does not fail: it looks up terms of one release in a map built
from another and misses quietly.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms._coordinator import (
    _refuse_a_snapshot_the_terms_do_not_come_from,
)
from tests.helpers.prediction_sets import make_parents


def _another_snapshot(session: Session) -> uuid.UUID:
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )

    other = OntologySnapshot(obo_url="file:///other", obo_version="other")
    session.add(other)
    session.flush()
    return other.id


def test_a_mismatched_snapshot_is_refused(postgres_url: str) -> None:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        parents = make_parents(session)
        other = _another_snapshot(session)

        # The one the annotation set draws from: allowed, and silent.
        _refuse_a_snapshot_the_terms_do_not_come_from(
            session, parents["annotation"], parents["snapshot"]
        )

        with pytest.raises(ValueError) as caught:
            _refuse_a_snapshot_the_terms_do_not_come_from(
                session, parents["annotation"], other
            )
        message = str(caught.value)
        # Both ids, because which one is wrong is the operator's question and
        # it cannot be answered from one of them.
        assert str(other) in message and str(parents["snapshot"]) in message

        session.rollback()


def test_an_unknown_annotation_set_is_left_to_its_own_check(postgres_url: str) -> None:
    """This guard does not duplicate the existence checks above it.

    ``_validate_inputs`` already refuses an annotation set that is not there,
    with a message about the annotation set. Raising a second, differently
    worded error here would send a reader looking at the ontology.
    """
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        _refuse_a_snapshot_the_terms_do_not_come_from(
            session, uuid.uuid4(), uuid.uuid4()
        )
        session.rollback()
