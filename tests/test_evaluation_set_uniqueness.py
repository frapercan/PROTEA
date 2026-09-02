"""Tests for the ``EvaluationSet`` pair-uniqueness contract.

Layer 1 (schema): the identity is UNIQUE at the DB level -- the pair of
annotation sets AND the three snapshots that decide the delta. The pair alone
was the key until it turned out not to be an identity: the same pair read under
a different propagation graph is a different measurement, by 21 percent of the
PK bucket's annotations on the 220 -> 227 window.

Widening it reopens an ambiguity the narrow key used to make impossible, so the
caller that relied on it -- ``_resolve_train_split_eval``, whose
``.one_or_none()`` on the bare pair was the root cause of the LB.1 vanilla
incident on 2026-05-12 -- now collects candidates and names them instead of
letting SQLAlchemy raise ``MultipleResultsFound`` from three frames down.

Layer 2 (operation): ``GenerateEvaluationSetOperation`` short-circuits to the
existing row when one exists under the SAME identity, keeping ``POST /jobs``
idempotent; a submission that changes a propagation graph finds nothing and
computes, which is the behaviour the widened key exists to allow.
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


def _seed_annotation_sets(session: Session) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID]:
    """Insert two OntologySnapshots + two AnnotationSet rows.

    Two snapshots, not one: every test below has to be able to vary a
    propagation graph while holding the pair fixed, which is the whole point of
    the widened key.
    """
    snaps = [
        OntologySnapshot(obo_url="http://example/go.obo", obo_version=v)
        for v in ("releases/2024-01-17", "releases/2025-01-17")
    ]
    session.add_all(snaps)
    session.flush()
    old_as = AnnotationSet(source="goa", source_version="226", ontology_snapshot_id=snaps[0].id)
    new_as = AnnotationSet(source="goa", source_version="230", ontology_snapshot_id=snaps[0].id)
    session.add_all([old_as, new_as])
    session.flush()
    return old_as.id, new_as.id, snaps[0].id, snaps[1].id


def _eval_set(old_id, new_id, pivot, old_native, new_native) -> EvaluationSet:
    """An EvaluationSet spelled out by every field of its identity."""
    return EvaluationSet(
        old_annotation_set_id=old_id,
        new_annotation_set_id=new_id,
        pivot_snapshot_id=pivot,
        old_native_snapshot_id=old_native,
        new_native_snapshot_id=new_native,
        stats={},
    )


# ---------------------------------------------------------------------------
# Layer 1: schema UNIQUE constraint
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_identical_identity_rejected_by_unique_constraint(pg_session: Session) -> None:
    """Same pair AND same three graphs is the same measurement twice."""
    old_id, new_id, s1, _ = _seed_annotation_sets(pg_session)

    pg_session.add(_eval_set(old_id, new_id, s1, s1, s1))
    pg_session.commit()

    pg_session.add(_eval_set(old_id, new_id, s1, s1, s1))
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


@pytest.mark.integration
def test_same_pair_under_different_graphs_coexist(pg_session: Session) -> None:
    """The reason the key was widened.

    One row propagates both sides under the pivot; the other reads the new side
    under its own native DAG. They hold different deltas over the same two
    annotation sets, and a schema that admitted only one of them would force a
    choice the evidence has not yet settled.
    """
    old_id, new_id, s1, s2 = _seed_annotation_sets(pg_session)

    pg_session.add(_eval_set(old_id, new_id, s1, s1, s1))
    pg_session.add(_eval_set(old_id, new_id, s1, s1, s2))
    pg_session.commit()

    assert pg_session.query(EvaluationSet).count() == 2


@pytest.mark.integration
def test_swapped_pair_is_a_different_row(pg_session: Session) -> None:
    """``(A, B)`` and ``(B, A)`` are distinct evaluation episodes."""
    old_id, new_id, s1, _ = _seed_annotation_sets(pg_session)

    pg_session.add(_eval_set(old_id, new_id, s1, s1, s1))
    pg_session.add(_eval_set(new_id, old_id, s1, s1, s1))
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
