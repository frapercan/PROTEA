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
    dependency_conflicts,
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

        # Same repository on both sides, but this set records no sibling
        # commits, so the second half of the comparison cannot be made. It says
        # so and runs. The silent case, where both halves agree, is asserted in
        # test_a_stale_sibling_is_refused_even_when_the_repository_matches.
        events = _run_guard(session, agreeing, _RUNNING, monkeypatch)
        assert _emitted(events) == {"predict_go_terms_batch.dependencies_unverifiable"}

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


def test_a_stale_sibling_is_refused_even_when_the_repository_matches(
    postgres_url: str, monkeypatch
) -> None:
    """The repository commit does not identify the code.

    On 2026-08-29 a node held the correct PROTEA tree with a stale
    ``protea-method`` installed, and both builds called themselves 0.3.1, so no
    version check anywhere could see it. The witness is the resolved commit,
    and this asserts the guard reads it.
    """
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    pinned = {"protea-method": "c" * 40, "protea-sources": "d" * 40}
    stale = {"protea-method": "e" * 40, "protea-sources": "d" * 40}

    with Session(engine) as session:
        parents = make_parents(session)
        opened = make_prediction_set(
            session,
            parents,
            {"code_revision": _RUNNING, "dependency_revisions": pinned},
        )
        no_deps = make_prediction_set(session, parents, {"code_revision": _RUNNING})

        monkeypatch.setattr(
            "protea.core.operations.predict_go_terms._batch_op.dependency_revisions",
            lambda: stale,
        )
        with pytest.raises(ForeignRevisionError) as caught:
            _run_guard(session, opened, _RUNNING, monkeypatch)
        message = str(caught.value)
        # The sibling is named, and the one that agrees is not, because the
        # operator has to know which to reinstall.
        assert "protea-method" in message
        assert "protea-sources" not in message

        # The same repository and the same siblings: silent.
        monkeypatch.setattr(
            "protea.core.operations.predict_go_terms._batch_op.dependency_revisions",
            lambda: pinned,
        )
        assert _run_guard(session, opened, _RUNNING, monkeypatch) == []

        # A set opened before dependencies were recorded says so and runs.
        events = _run_guard(session, no_deps, _RUNNING, monkeypatch)
        assert _emitted(events) == {
            "predict_go_terms_batch.dependencies_unverifiable"
        }

        session.rollback()


def test_only_siblings_both_sides_name_can_disagree() -> None:
    """A sibling missing on one side is a different question.

    Answering it here would make an installer that leaves no direct_url.json
    fatal, and a guard that fails for a reason nobody can act on is a guard
    that gets switched off.
    """
    pinned = {"protea-method": "c" * 40}
    assert dependency_conflicts(pinned, {"protea-method": "e" * 40})
    assert dependency_conflicts(pinned, pinned) == {}
    assert dependency_conflicts(pinned, {"protea-sources": "d" * 40}) == {}
    assert dependency_conflicts(None, pinned) == {}
    assert dependency_conflicts(pinned, None) == {}


def test_the_receipt_carries_the_sibling_commits() -> None:
    """Recorded, and it is the recording the guard reads back."""
    from protea_contracts import PredictGOTermsPayload

    from protea.core.code_revision import dependency_revisions
    from protea.core.operations.predict_go_terms._receipt import run_receipt

    payload = PredictGOTermsPayload(
        embedding_config_id=str(uuid.uuid4()),
        annotation_set_id=str(uuid.uuid4()),
        ontology_snapshot_id=str(uuid.uuid4()),
    )
    receipt = run_receipt(payload, uuid.uuid4())
    assert receipt["dependency_revisions"] == dependency_revisions()
    # This checkout installs its siblings from git, so the map is not empty.
    # An empty map would make the assertion above pass while proving nothing.
    assert receipt["dependency_revisions"]
