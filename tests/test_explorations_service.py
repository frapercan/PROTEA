"""The server moves an exploration through its states, and logs every move (ADR-D55).

On SQLite with the four exploration tables, through the real ORM, so the
tables' CHECKs stand behind the service's own refusals. Whether an arm already
had results in the window reads ``evaluation_result`` and ``prediction_set``,
which SQLite cannot build; it is stubbed here and run for real on Postgres in
``tests/test_explorations_pg.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.explorations import rules_fingerprint
from protea.infrastructure.orm.base import Base
from protea.services import explorations_service as svc

TABLES = ("external_reference", "exploration", "exploration_link", "exploration_event")
LAB = svc.Actor(name="key:lab", type="api_key")
SERVER = svc.Actor(name="key:server", type="api_key")
AUTHOR = svc.Actor(name="author@example.org", type="user")
RULES = {"deciding_panels": ["NK/BPO"], "axis": "donor_policy", "decision_rule": "paired"}


@pytest.fixture()
def session(monkeypatch: pytest.MonkeyPatch) -> Any:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[Base.metadata.tables[t] for t in TABLES])
    monkeypatch.setattr(svc, "_had_results_already", lambda _s, _e: False)
    with Session(engine) as s:
        yield s


def _draft(session: Session, **over: Any) -> Any:
    fields: dict[str, Any] = {
        "title": "the instrument repeats",
        "question": "does the same run give the same panels?",
        "hypothesis": "two runs agree in every panel within the bootstrap interval",
        "kind": "instrument",
        "evaluation_set_id": uuid.uuid4(),
        "rules": RULES,
    }
    return svc.create(session, svc.Draft(**{**fields, **over}), LAB)


def _events(session: Session, row: Any) -> list[str]:
    return [e["event"] for e in svc.describe(session, row)["events"]]


def _reported(session: Session) -> Any:
    row = _draft(session)
    svc.launch(session, row, SERVER)
    row.state = "reported"  # the report operation is its own change
    session.flush()
    return row


class TestADraft:
    def test_it_is_numbered_after_the_last_one(self, session: Session) -> None:
        assert [_draft(session).number, _draft(session).number] == [1, 2]

    def test_it_logs_its_creation(self, session: Session) -> None:
        row = _draft(session)
        assert _events(session, row) == ["created"]
        assert row.proposer == "key:lab"

    def test_an_unknown_kind_or_an_empty_question_is_refused(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationError, match="kind"):
            _draft(session, kind="survey")
        with pytest.raises(svc.ExplorationError, match="question"):
            _draft(session, question="  ")

    def test_its_rules_can_be_replaced_while_it_is_a_draft(self, session: Session) -> None:
        row = _draft(session)
        svc.edit_rules(session, row, {"axis": "length"}, LAB)
        assert row.rules == {"axis": "length"}

    def test_it_is_found_by_number_or_by_id(self, session: Session) -> None:
        row = _draft(session)
        assert svc.find(session, "1") is row
        assert svc.find(session, str(row.id)) is row
        with pytest.raises(svc.ExplorationNotFound):
            svc.find(session, "7")


class TestLaunching:
    def test_it_freezes_the_rules_with_their_fingerprint_time_and_launcher(
        self, session: Session
    ) -> None:
        row = _draft(session)
        svc.launch(session, row, SERVER)
        assert row.state == "running"
        assert row.rules_sha256 == rules_fingerprint(RULES)
        assert row.rules_frozen_at is not None
        assert row.rules_frozen_by == row.launcher == "key:server"
        assert _events(session, row) == ["created", "launched"]

    def test_the_proposer_does_not_launch(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationError, match="other than the proposer"):
            svc.launch(session, _draft(session), LAB)

    def test_rules_frozen_after_a_number_was_readable_are_post_hoc(
        self, session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(svc, "_had_results_already", lambda _s, _e: True)
        row = _draft(session)
        svc.launch(session, row, SERVER)
        assert row.post_hoc is True
        assert svc.describe(session, row)["events"][-1]["body"]["post_hoc"] is True

    def test_a_running_exploration_is_not_launched_again_nor_edited(self, session: Session) -> None:
        row = _draft(session)
        svc.launch(session, row, SERVER)
        with pytest.raises(svc.ExplorationConflict):
            svc.launch(session, row, AUTHOR)
        with pytest.raises(svc.ExplorationConflict):
            svc.edit_rules(session, row, {}, LAB)


class TestAmending:
    def test_an_amendment_carries_both_fingerprints_and_why(self, session: Session) -> None:
        row = _draft(session)
        svc.launch(session, row, SERVER)
        before = row.rules_sha256
        svc.amend(session, row, {**RULES, "floor": 30}, "the floor was missing", LAB)
        body = svc.describe(session, row)["events"][-1]["body"]
        assert body == {"from": before, "to": row.rules_sha256, "reason": "the floor was missing"}

    def test_the_same_rules_or_no_reason_are_refused(self, session: Session) -> None:
        row = _draft(session)
        svc.launch(session, row, SERVER)
        with pytest.raises(svc.ExplorationError, match="already frozen"):
            svc.amend(session, row, RULES, "no change", LAB)
        with pytest.raises(svc.ExplorationError, match="says why"):
            svc.amend(session, row, {"x": 1}, " ", LAB)

    def test_a_draft_is_edited_not_amended(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationConflict):
            svc.amend(session, _draft(session), {"x": 1}, "why", LAB)


class TestLinking:
    def test_one_target_is_linked(self, session: Session) -> None:
        row = _draft(session)
        target = uuid.uuid4()
        svc.link(
            session,
            row,
            svc.LinkSpec(role="arm", arm="base", target={"prediction_set_id": str(target)}),
            LAB,
        )
        links = svc.describe(session, row)["links"]
        assert links[0]["target"] == {"prediction_set_id": str(target)}
        assert links[0]["arm"] == "base"

    @pytest.mark.parametrize(
        ("target", "match"),
        [
            ({}, "exactly one"),
            ({"job_id": str(uuid.uuid4()), "query_set_id": str(uuid.uuid4())}, "exactly one"),
            ({"store_key": "eval_artifacts/x"}, "sha256"),
            ({"winner_id": str(uuid.uuid4())}, "unknown"),
            ({"job_id": "not-an-id"}, "not an id"),
        ],
    )
    def test_a_link_that_is_not_one_target_is_refused(
        self, session: Session, target: dict[str, Any], match: str
    ) -> None:
        with pytest.raises(svc.ExplorationError, match=match):
            svc.link(session, _draft(session), svc.LinkSpec(role="input", target=target), LAB)

    def test_a_temporary_target_needs_its_expiry(self, session: Session) -> None:
        row = _draft(session)
        target = {"job_id": str(uuid.uuid4())}
        with pytest.raises(svc.ExplorationError, match="expiry"):
            svc.link(session, row, svc.LinkSpec(role="temporary", target=target), LAB)
        svc.link(
            session,
            row,
            svc.LinkSpec(
                role="temporary", target=target, expires_at=datetime(2026, 12, 1, tzinfo=UTC)
            ),
            LAB,
        )

    def test_an_exploration_does_not_link_to_itself(self, session: Session) -> None:
        row = _draft(session)
        with pytest.raises(svc.ExplorationError, match="itself"):
            svc.link(
                session,
                row,
                svc.LinkSpec(role="supports", target={"target_exploration_id": str(row.id)}),
                LAB,
            )

    def test_a_reported_exploration_takes_no_new_links(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationConflict):
            svc.link(
                session,
                _reported(session),
                svc.LinkSpec(role="input", target={"job_id": str(uuid.uuid4())}),
                LAB,
            )


class TestReviewAndDecision:
    def test_only_objections_replies_and_checks_are_appended_by_hand(
        self, session: Session
    ) -> None:
        row = _reported(session)
        svc.append(session, row, event="objection", body={"cell": "NK/BPO"}, actor=SERVER)
        with pytest.raises(svc.ExplorationError, match="by hand"):
            svc.append(session, row, event="decision", body={}, actor=SERVER)

    def test_the_proposer_does_not_review(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationError, match="other than the proposer"):
            svc.review(session, _reported(session), LAB)

    def test_the_decision_needs_the_review_first(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationConflict, match="reviewed"):
            svc.decide(session, _reported(session), "accepted", "it holds", AUTHOR)

    def test_an_api_key_does_not_decide(self, session: Session) -> None:
        row = _reported(session)
        svc.review(session, row, SERVER)
        with pytest.raises(svc.ExplorationError, match="user session"):
            svc.decide(session, row, "accepted", "it holds", SERVER)

    def test_the_author_decides_once_reviewed(self, session: Session) -> None:
        row = _reported(session)
        svc.review(session, row, SERVER)
        svc.decide(session, row, "rejected", "an accepted against is a result too", AUTHOR)
        assert (row.state, row.decision, row.decided_by) == ("decided", "rejected", AUTHOR.name)
        assert _events(session, row)[-2:] == ["reviewed", "decision"]

    def test_a_decision_outside_the_vocabulary_or_without_a_reason_is_refused(
        self, session: Session
    ) -> None:
        row = _reported(session)
        svc.review(session, row, SERVER)
        with pytest.raises(svc.ExplorationError, match="decision"):
            svc.decide(session, row, "maybe", "unsure", AUTHOR)
        with pytest.raises(svc.ExplorationError, match="says why"):
            svc.decide(session, row, "accepted", "", AUTHOR)


class TestArchiving:
    def test_archived_is_a_reason_and_it_stops_every_move(self, session: Session) -> None:
        row = _draft(session)
        svc.archive(session, row, "abandoned", LAB)
        assert row.archived_reason == "abandoned"
        with pytest.raises(svc.ExplorationConflict, match="archived"):
            svc.launch(session, row, SERVER)
        with pytest.raises(svc.ExplorationConflict, match="already archived"):
            svc.archive(session, row, "closed", LAB)

    def test_an_unknown_reason_is_refused(self, session: Session) -> None:
        with pytest.raises(svc.ExplorationError, match="reason"):
            svc.archive(session, _draft(session), "forgotten", LAB)

    def test_the_listing_hides_archived_unless_asked(self, session: Session) -> None:
        kept, gone = _draft(session), _draft(session)
        svc.archive(session, gone, "withdrawn", LAB)
        assert [r["number"] for r in svc.listing(session)] == [kept.number]
        assert len(svc.listing(session, include_archived=True)) == 2


def test_an_actor_says_what_it_is() -> None:
    with pytest.raises(svc.ExplorationError):
        svc.Actor(name="x", type="robot")
    with pytest.raises(svc.ExplorationError):
        svc.Actor(name="", type="user")


def test_the_answer_to_a_creation_carries_its_event_without_autoflush() -> None:
    # PROTEA builds its sessions with autoflush=False; the create route answers
    # with describe() in the same transaction, which must already see the event.
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[Base.metadata.tables[t] for t in TABLES])
    with Session(engine, autoflush=False) as s:
        row = _draft(s)
        assert [e["event"] for e in svc.describe(s, row)["events"]] == ["created"]
