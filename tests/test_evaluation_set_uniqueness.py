"""Tests for the ``EvaluationSet`` pair-uniqueness contract.

Layer 1 (schema): the ``(old_annotation_set_id, new_annotation_set_id)``
pair is enforced UNIQUE at the DB level so that the dump pipeline's
``.one_or_none()`` lookup at
``protea/core/training_dump_helpers.py:_resolve_train_split_eval`` can
never trip ``MultipleResultsFound`` (root cause of the LB.1 vanilla
incident on 2026-05-12).

Layer 2 (operation): ``GenerateEvaluationSetOperation`` short-circuits
to the existing row when an EvaluationSet for the pair already exists,
keeping ``POST /jobs`` submissions idempotent without bouncing off the
DB constraint.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.core.evaluation import EvaluationData
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetOperation,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


@pytest.fixture()
def pg_session(postgres_url: str):
    """Fresh schema + a session bound to the live engine."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


def _seed_annotation_sets(session: Session) -> tuple[uuid.UUID, uuid.UUID]:
    """Insert one OntologySnapshot + two AnnotationSet rows."""
    snap = OntologySnapshot(
        obo_url="http://example/go.obo",
        obo_version="releases/2024-01-17",
    )
    session.add(snap)
    session.flush()
    old_as = AnnotationSet(source="goa", source_version="226", ontology_snapshot_id=snap.id)
    new_as = AnnotationSet(source="goa", source_version="230", ontology_snapshot_id=snap.id)
    session.add_all([old_as, new_as])
    session.flush()
    return old_as.id, new_as.id


# ---------------------------------------------------------------------------
# Layer 1: schema UNIQUE constraint
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_duplicate_pair_rejected_by_unique_constraint(pg_session: Session) -> None:
    """Inserting two ``EvaluationSet`` rows for the same (old, new) pair
    must raise ``IntegrityError`` thanks to the UNIQUE constraint added
    in alembic revision ``b8e3f1a7c2d9``.
    """
    old_id, new_id = _seed_annotation_sets(pg_session)

    pg_session.add(
        EvaluationSet(
            old_annotation_set_id=old_id,
            new_annotation_set_id=new_id,
            stats={},
        )
    )
    pg_session.commit()

    pg_session.add(
        EvaluationSet(
            old_annotation_set_id=old_id,
            new_annotation_set_id=new_id,
            stats={},
        )
    )
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


@pytest.mark.integration
def test_swapped_pair_is_a_different_row(pg_session: Session) -> None:
    """``(A, B)`` and ``(B, A)`` are distinct evaluation episodes."""
    old_id, new_id = _seed_annotation_sets(pg_session)

    pg_session.add(
        EvaluationSet(
            old_annotation_set_id=old_id,
            new_annotation_set_id=new_id,
            stats={},
        )
    )
    pg_session.add(
        EvaluationSet(
            old_annotation_set_id=new_id,
            new_annotation_set_id=old_id,
            stats={},
        )
    )
    # Both rows commit cleanly — the swap is a legitimately distinct pair.
    pg_session.commit()
    assert pg_session.query(EvaluationSet).count() == 2


# ---------------------------------------------------------------------------
# Layer 2: operation idempotency
# ---------------------------------------------------------------------------


def _make_annotation_set_mock(snapshot_id: uuid.UUID) -> MagicMock:
    s = MagicMock()
    s.ontology_snapshot_id = snapshot_id
    return s


def _make_eval_data() -> EvaluationData:
    return EvaluationData(
        nk={"P1": {"GO:0001"}},
        lk={"P2": {"GO:0002"}},
        pk={},
    )


def test_generate_evaluation_set_returns_existing_on_resubmit() -> None:
    """When an EvaluationSet already exists for the pair, the operation
    short-circuits to its id rather than re-computing and inserting a
    duplicate. The artifact store is left untouched.
    """
    op = GenerateEvaluationSetOperation()
    emit = MagicMock()

    session = MagicMock()
    snap_id = uuid.uuid4()
    old_set = _make_annotation_set_mock(snap_id)
    new_set = _make_annotation_set_mock(snap_id)
    session.get.side_effect = [old_set, new_set]

    existing = MagicMock()
    existing.id = uuid.uuid4()
    existing.groundtruth_uri = "s3://protea/groundtruth/existing.parquet"
    existing.stats = {"nk_proteins": 7, "lk_proteins": 3, "mode": "same_snapshot"}
    session.query.return_value.filter_by.return_value.one_or_none.return_value = existing

    payload = {
        "old_annotation_set_id": str(uuid.uuid4()),
        "new_annotation_set_id": str(uuid.uuid4()),
    }

    with (
        patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
        ) as mock_same,
        patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data_reconciled",
        ) as mock_reconciled,
        patch(
            "protea.core.operations.generate_evaluation_set.get_artifact_store",
        ) as mock_store,
    ):
        result = op.execute(session, payload, emit=emit)

    assert result.result["evaluation_set_id"] == str(existing.id)
    assert result.result["groundtruth_uri"] == existing.groundtruth_uri
    assert result.result["nk_proteins"] == 7
    # Neither delta-compute path runs, no artifact store call, no INSERT.
    assert not mock_same.called
    assert not mock_reconciled.called
    assert not mock_store.called
    assert not session.add.called
    # An idempotent_reuse event is emitted.
    events = [call.args[0] for call in emit.call_args_list]
    assert "generate_evaluation_set.idempotent_reuse" in events


def test_generate_evaluation_set_inserts_when_no_existing_row() -> None:
    """When no existing EvaluationSet covers the pair, the operation
    proceeds with the normal compute + insert path.
    """
    op = GenerateEvaluationSetOperation()
    emit = MagicMock()

    session = MagicMock()
    snap_id = uuid.uuid4()
    old_set = _make_annotation_set_mock(snap_id)
    new_set = _make_annotation_set_mock(snap_id)
    session.get.side_effect = [old_set, new_set]
    session.query.return_value.filter_by.return_value.one_or_none.return_value = None

    def add_side(obj):
        obj.id = uuid.uuid4()

    session.add.side_effect = add_side
    session.flush = MagicMock()

    with (
        patch(
            "protea.core.operations.generate_evaluation_set.compute_evaluation_data",
            return_value=_make_eval_data(),
        ) as mock_same,
        patch(
            "protea.core.operations.generate_evaluation_set.get_artifact_store",
            return_value=MagicMock(),
        ),
        patch(
            "protea.core.operations.generate_evaluation_set.load_settings",
            return_value=MagicMock(),
        ),
    ):
        result = op.execute(
            session,
            {
                "old_annotation_set_id": str(uuid.uuid4()),
                "new_annotation_set_id": str(uuid.uuid4()),
            },
            emit=emit,
        )

    assert mock_same.called
    assert session.add.called
    assert "evaluation_set_id" in result.result
    events = [call.args[0] for call in emit.call_args_list]
    assert "generate_evaluation_set.start" in events
    assert "generate_evaluation_set.done" in events
    assert "generate_evaluation_set.idempotent_reuse" not in events
