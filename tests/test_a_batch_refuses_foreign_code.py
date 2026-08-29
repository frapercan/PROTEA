"""A batch will not add rows to a set another revision opened.

WHY THIS TEST EXISTS. On 2026-08-29 a compute node ran a revision that predated
the donor-policy fix and wrote 193,303 rows into a prediction set the server
believed was homogeneous. The run reported success. The mismatch was found days
later by reading a log file on the other machine, because nothing in the
database could say which code had produced which rows.

The test therefore drives the guard the way that incident did: a set opened
under one revision, a worker running another. It asserts the refusal, and it
asserts the two cases that must NOT refuse, because a guard that fails a clean
laptop on every edit would be turned off within a day.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.code_revision import (
    DIRTY_SUFFIX,
    UNKNOWN,
    ForeignRevisionError,
    is_identifying,
    revisions_conflict,
)
from protea.core.operations.predict_go_terms._batch_op import (
    PredictGOTermsBatchOperation,
)
from tests.helpers.prediction_sets import make_parents, make_prediction_set

_OPENED_BY = "1111111111111111111111111111111111111111"
_RUNNING = "2222222222222222222222222222222222222222"


class _Ctx:
    """Only the field the guard reads. The real context carries fifteen."""

    def __init__(self, prediction_set_id: uuid.UUID) -> None:
        self.prediction_set_id = prediction_set_id


def _emitted(events: list[tuple]) -> set[str]:
    return {e[0] for e in events}


def _run_guard(session: Session, set_id: uuid.UUID, running: str, monkeypatch) -> list[tuple]:
    events: list[tuple] = []
    monkeypatch.setattr(
        "protea.core.operations.predict_go_terms._batch_op.code_revision",
        lambda: running,
    )
    PredictGOTermsBatchOperation._refuse_foreign_revision(
        session, _Ctx(set_id), lambda *a: events.append(a)
    )
    return events


def test_a_batch_refuses_a_set_another_revision_opened(postgres_url: str, monkeypatch) -> None:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        parents = make_parents(session)

        def opened_by(revision: str | None) -> uuid.UUID:
            meta = {} if revision is None else {"code_revision": revision}
            return make_prediction_set(session, parents, meta)

        conflicting = opened_by(_OPENED_BY)
        agreeing = opened_by(_RUNNING)
        dirty = opened_by(_OPENED_BY + DIRTY_SUFFIX)
        silent = opened_by(None)

        with pytest.raises(ForeignRevisionError) as caught:
            _run_guard(session, conflicting, _RUNNING, monkeypatch)
        # Both revisions in the message: which one is wrong is the operator's
        # question, and it cannot be answered from one of them.
        assert _OPENED_BY in str(caught.value) and _RUNNING in str(caught.value)

        # Same code on both sides: no event at all. A guard that narrates every
        # agreement buries the one message that matters.
        assert _run_guard(session, agreeing, _RUNNING, monkeypatch) == []

        # Cannot be established either way. Says so, and lets the batch run.
        for unverifiable in (dirty, silent):
            events = _run_guard(session, unverifiable, _RUNNING, monkeypatch)
            assert _emitted(events) == {"predict_go_terms_batch.revision_unverifiable"}

        # A clean set and a dirty worker is the same unanswered question seen
        # from the other side, and must not raise either.
        events = _run_guard(session, agreeing, _RUNNING + DIRTY_SUFFIX, monkeypatch)
        assert _emitted(events) == {"predict_go_terms_batch.revision_unverifiable"}

        session.rollback()


def test_only_two_clean_revisions_can_disagree() -> None:
    """The truth table, stated once where it can be read.

    ``revisions_conflict`` returning False is not agreement. Three of these
    five rows are unanswered questions, and the guard reports them differently
    for that reason.
    """
    assert revisions_conflict(_OPENED_BY, _RUNNING) is True
    assert revisions_conflict(_OPENED_BY, _OPENED_BY) is False
    assert revisions_conflict(_OPENED_BY + DIRTY_SUFFIX, _RUNNING) is False
    assert revisions_conflict(_OPENED_BY, UNKNOWN) is False
    assert revisions_conflict(None, _RUNNING) is False

    assert is_identifying(_OPENED_BY) is True
    assert is_identifying(_OPENED_BY + DIRTY_SUFFIX) is False
    assert is_identifying(UNKNOWN) is False
    assert is_identifying(None) is False


def test_a_dirty_tree_never_passes_for_a_commit(monkeypatch) -> None:
    """The recorded value has to say the tree was edited.

    The fleet on this machine runs out of the working tree, so a sha read there
    names a commit the running code is not. Recording it bare would let two
    different edits of one commit compare equal, which is a comparison that
    passes for the wrong reason.
    """
    import protea.core.code_revision as cr

    monkeypatch.delenv(cr.ENV_VAR, raising=False)
    monkeypatch.setattr(cr, "resolve_protea_git_sha", lambda: _OPENED_BY)

    monkeypatch.setattr(cr, "_working_tree_is_dirty", lambda: False)
    assert cr.code_revision() == _OPENED_BY

    monkeypatch.setattr(cr, "_working_tree_is_dirty", lambda: True)
    assert cr.code_revision() == _OPENED_BY + DIRTY_SUFFIX
    assert is_identifying(cr.code_revision()) is False

    # git unavailable: not a commit either, and not silently a clean one.
    monkeypatch.setattr(cr, "_working_tree_is_dirty", lambda: None)
    assert cr.code_revision() == UNKNOWN

    # A deployment with no .git can still declare what it runs.
    monkeypatch.setenv(cr.ENV_VAR, _RUNNING)
    assert cr.code_revision() == _RUNNING


def test_the_receipt_carries_the_revision() -> None:
    """Recording it is half; the batch guard above is the other half."""
    from protea_contracts import PredictGOTermsPayload

    from protea.core.operations.predict_go_terms._receipt import run_receipt

    payload = PredictGOTermsPayload(
        embedding_config_id=str(uuid.uuid4()),
        annotation_set_id=str(uuid.uuid4()),
        ontology_snapshot_id=str(uuid.uuid4()),
    )
    receipt = run_receipt(payload, uuid.uuid4())
    assert receipt["code_revision"] == __import__(
        "protea.core.code_revision", fromlist=["code_revision"]
    ).code_revision()
