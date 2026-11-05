"""Evaluated explorations, the server's side (ADR-D55).

The tables hold the state machine on their own: only a draft has open rules,
the freeze is recorded whole, only a decided exploration carries a decision.
This module is the one writer that moves an exploration through those states,
and every move writes its event in the same transaction, so the log and the
row cannot disagree about what happened.

What is not here yet, and is each its own change: the receipt computed while
the arms are chosen, which will refuse a launch whose arms differ in more than
the declared axis; the report generated from rows; the checklist and the
grade; retention; and launching a step from the page, which creates its link
in the transaction that enqueues it.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import exists, func, or_, select
from sqlalchemy.orm import Session

from protea.core.explorations import (
    ACTOR_TYPES,
    ARCHIVE_REASONS,
    DECISIONS,
    KINDS,
    LINK_ROLES,
    LINK_TARGETS,
    TRANSITIONS,
    rules_fingerprint,
)
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.exploration.exploration import Exploration
from protea.infrastructure.orm.models.exploration.exploration_event import ExplorationEvent
from protea.infrastructure.orm.models.exploration.exploration_link import ExplorationLink

__all__ = [
    "FREE_EVENTS",
    "Actor",
    "Draft",
    "ExplorationConflict",
    "ExplorationError",
    "ExplorationNotFound",
    "LinkSpec",
    "amend",
    "append",
    "archive",
    "create",
    "decide",
    "describe",
    "edit_rules",
    "find",
    "launch",
    "link",
    "listing",
    "report",
    "review",
]

#: Events a principal may append by hand. Every other event is written by the
#: move it records, so the log cannot claim a move the row did not make.
FREE_EVENTS: tuple[str, ...] = ("objection", "reply", "check_answered")


class ExplorationError(ValueError):
    """A request the exploration's rules refuse. The router answers 422."""


class ExplorationNotFound(LookupError):
    """No exploration by that id or number. The router answers 404."""


class ExplorationConflict(RuntimeError):
    """A move the current state does not allow. The router answers 409."""


@dataclass(frozen=True)
class Actor:
    """Who is acting, and as what: the author's session is not an API key."""

    name: str
    type: str

    def __post_init__(self) -> None:
        if self.type not in ACTOR_TYPES:
            raise ExplorationError(f"actor type must be one of {ACTOR_TYPES}")
        if not self.name:
            raise ExplorationError("an actor needs a name")


@dataclass(frozen=True)
class Draft:
    """What a new exploration states before anything runs."""

    title: str
    question: str
    hypothesis: str
    kind: str
    evaluation_set_id: uuid.UUID
    rules: dict[str, Any]


@dataclass(frozen=True)
class LinkSpec:
    """One object to link: its role, exactly one target, and what retention needs."""

    role: str
    target: dict[str, Any]
    arm: str | None = None
    owned: bool = False
    expires_at: datetime | None = None
    size_bytes: int | None = None


def find(session: Session, ref: str) -> Exploration:
    """An exploration by its id or by the number a person says."""
    if ref.isdigit():
        row = session.execute(
            select(Exploration).where(Exploration.number == int(ref))
        ).scalar_one_or_none()
    else:
        try:
            row = session.get(Exploration, uuid.UUID(ref))
        except ValueError as exc:
            raise ExplorationNotFound(f"{ref!r} is neither an id nor a number") from exc
    if row is None:
        raise ExplorationNotFound(f"no exploration {ref!r}")
    return row


def _event(
    session: Session,
    exploration: Exploration,
    event: str,
    actor: Actor,
    body: dict[str, Any] | None = None,
    link_id: uuid.UUID | None = None,
) -> ExplorationEvent:
    row = ExplorationEvent(
        exploration_id=exploration.id,
        actor=actor.name,
        actor_type=actor.type,
        event=event,
        body=body or {},
        link_id=link_id,
    )
    session.add(row)
    return row


def _require_state(exploration: Exploration, *states: str) -> None:
    if exploration.archived_reason is not None:
        raise ExplorationConflict(
            f"exploration {exploration.number} is archived ({exploration.archived_reason})"
        )
    if exploration.state not in states:
        raise ExplorationConflict(
            f"exploration {exploration.number} is {exploration.state}; this needs "
            f"{' or '.join(states)}"
        )


def _move(exploration: Exploration, to: str) -> None:
    if TRANSITIONS.get(exploration.state) != to:
        raise ExplorationConflict(f"{exploration.state} does not move to {to}")
    exploration.state = to


def create(session: Session, draft: Draft, actor: Actor) -> Exploration:
    """A draft, numbered after the last one, with its ``created`` event."""
    if draft.kind not in KINDS:
        raise ExplorationError(f"kind must be one of {KINDS}")
    for name in ("title", "question", "hypothesis"):
        if not getattr(draft, name).strip():
            raise ExplorationError(f"{name} must say something")
    number = (session.execute(select(func.max(Exploration.number))).scalar() or 0) + 1
    row = Exploration(
        number=number,
        title=draft.title.strip(),
        question=draft.question.strip(),
        hypothesis=draft.hypothesis.strip(),
        kind=draft.kind,
        evaluation_set_id=draft.evaluation_set_id,
        rules=draft.rules,
        proposer=actor.name,
    )
    session.add(row)
    session.flush()
    _event(session, row, "created", actor, {"kind": draft.kind, "number": number})
    return row


def edit_rules(
    session: Session, exploration: Exploration, rules: dict[str, Any], actor: Actor
) -> None:
    """Replace a draft's rules. Once launched, only an amendment changes them."""
    _require_state(exploration, "draft")
    exploration.rules = rules
    session.flush()


def _had_results_already(session: Session, exploration: Exploration) -> bool:
    """Whether an arm already had results in the window when the rules are frozen.

    An arm is linked either as a prediction set or as the embedding config its
    sets were made under. One semi-join, through the window's index: it asks
    whether such a result exists, not how many.
    """
    arm = (ExplorationLink.exploration_id == exploration.id) & (ExplorationLink.role == "arm")
    arm_sets = select(ExplorationLink.prediction_set_id).where(
        arm, ExplorationLink.prediction_set_id.is_not(None)
    )
    arm_configs = select(ExplorationLink.embedding_config_id).where(
        arm, ExplorationLink.embedding_config_id.is_not(None)
    )
    query = select(
        exists().where(
            EvaluationResult.evaluation_set_id == exploration.evaluation_set_id,
            EvaluationResult.prediction_set_id == PredictionSet.id,
            or_(
                PredictionSet.id.in_(arm_sets),
                PredictionSet.embedding_config_id.in_(arm_configs),
            ),
        )
    )
    return bool(session.execute(query).scalar())


def _refuse_crossed_arms(session: Session, exploration: Exploration) -> None:
    """Refuse when the arms already differ in more than the declared axis."""
    from protea.services.exploration_receipt import launch_blockers

    blockers, layer = launch_blockers(session, exploration)
    if blockers:
        raise ExplorationError(f"{blockers[0]}: {layer.get('detail', '')}")


def launch(session: Session, exploration: Exploration, actor: Actor) -> None:
    """Freeze the rules with their fingerprint, the database time and the launcher.

    The launcher must be a principal other than the proposer: whoever wrote the
    rules does not also decide they are ready. ``post_hoc`` is set when an arm
    already had results in the window, because rules frozen after a number was
    readable are not rules written before it.
    """
    _require_state(exploration, "draft")
    if actor.name == exploration.proposer:
        raise ExplorationError("the launcher must be a principal other than the proposer")
    _refuse_crossed_arms(session, exploration)
    fingerprint = rules_fingerprint(exploration.rules)
    post_hoc = _had_results_already(session, exploration)
    _move(exploration, "running")
    exploration.rules_sha256 = fingerprint
    exploration.rules_frozen_at = func.now()  # type: ignore[assignment]
    exploration.rules_frozen_by = actor.name
    exploration.launcher = actor.name
    exploration.post_hoc = post_hoc
    _event(
        session, exploration, "launched", actor, {"rules_sha256": fingerprint, "post_hoc": post_hoc}
    )
    session.flush()


def amend(
    session: Session, exploration: Exploration, rules: dict[str, Any], reason: str, actor: Actor
) -> None:
    """Change frozen rules, as an event that carries both fingerprints and why."""
    _require_state(exploration, "running")
    if not reason.strip():
        raise ExplorationError("an amendment says why")
    before = exploration.rules_sha256
    after = rules_fingerprint(rules)
    if after == before:
        raise ExplorationError("the amended rules are the rules already frozen")
    exploration.rules = rules
    exploration.rules_sha256 = after
    _event(
        session, exploration, "amendment", actor, {"from": before, "to": after, "reason": reason}
    )
    session.flush()


def _target_of(target: dict[str, Any]) -> dict[str, Any]:
    unknown = set(target) - {*LINK_TARGETS, "store_sha256"}
    if unknown:
        raise ExplorationError(f"unknown link targets {sorted(unknown)}; known: {LINK_TARGETS}")
    named = [k for k in LINK_TARGETS if target.get(k) is not None]
    if len(named) != 1:
        raise ExplorationError(f"a link names exactly one target, not {len(named)}")
    if ("store_key" in named) != (target.get("store_sha256") is not None):
        raise ExplorationError("a store key is linked together with its sha256, and only then")
    out: dict[str, Any] = {}
    for key, value in target.items():
        if value is None:
            continue
        if key in ("store_key", "store_sha256"):
            out[key] = str(value)
            continue
        try:
            out[key] = uuid.UUID(str(value))
        except ValueError as exc:
            raise ExplorationError(f"{key} is not an id: {value!r}") from exc
    return out


def link(
    session: Session, exploration: Exploration, spec: LinkSpec, actor: Actor
) -> ExplorationLink:
    """Link one object to a draft or a running exploration."""
    _require_state(exploration, "draft", "running")
    if spec.role not in LINK_ROLES:
        raise ExplorationError(f"role must be one of {LINK_ROLES}")
    if spec.role == "temporary" and spec.expires_at is None:
        raise ExplorationError("a temporary target is linked with its expiry date")
    resolved = _target_of(spec.target)
    if resolved.get("target_exploration_id") == exploration.id:
        raise ExplorationError("an exploration does not link to itself")
    row = ExplorationLink(
        exploration_id=exploration.id,
        role=spec.role,
        arm=spec.arm,
        owned=spec.owned,
        expires_at=spec.expires_at,
        size_bytes=spec.size_bytes,
        **resolved,
    )
    session.add(row)
    session.flush()
    if spec.role == "arm" and exploration.state == "running":
        # The rules are frozen, so an arm that crosses the axis is refused here,
        # and the transaction that would have linked it rolls back.
        _refuse_crossed_arms(session, exploration)
    return row


def append(
    session: Session,
    exploration: Exploration,
    *,
    event: str,
    body: dict[str, Any],
    actor: Actor,
    link_id: uuid.UUID | None = None,
) -> ExplorationEvent:
    """An objection, a reply or an answered check, on a reported exploration."""
    if event not in FREE_EVENTS:
        raise ExplorationError(f"only {FREE_EVENTS} are appended by hand")
    _require_state(exploration, "reported")
    row = _event(session, exploration, event, actor, body, link_id)
    session.flush()
    return row


def report(session: Session, exploration: Exploration, actor: Actor) -> None:
    """Generate the report from rows and move a running exploration to ``reported``.

    The whole report goes into the ``report`` event with its sha256, so the log
    holds what the reviewer reads, and the server's verdict and grade are set
    on the row from the same report.
    """
    from protea.services.exploration_report import CHECKLIST_VERSION, build

    _require_state(exploration, "running")
    document, sha = build(session, exploration)
    _move(exploration, "reported")
    exploration.verdict = document["verdict"]
    exploration.grade = document["grade"]
    exploration.checklist_version = CHECKLIST_VERSION
    _event(session, exploration, "report", actor, {"sha256": sha, "report": document})
    session.flush()


def _last_report_sha(session: Session, exploration: Exploration) -> str | None:
    body = session.execute(
        select(ExplorationEvent.body)
        .where(
            ExplorationEvent.exploration_id == exploration.id,
            ExplorationEvent.event == "report",
        )
        .order_by(ExplorationEvent.id.desc())
        .limit(1)
    ).scalar()
    return (body or {}).get("sha256")


def review(session: Session, exploration: Exploration, actor: Actor) -> None:
    """The reviewer's ``reviewed`` event, which the decision requires.

    A proposer does not review their own exploration: the review is the
    second pair of eyes, and the same pair twice is one.
    """
    _require_state(exploration, "reported")
    if actor.name == exploration.proposer:
        raise ExplorationError("the reviewer must be a principal other than the proposer")
    exploration.reviewer = actor.name
    _event(session, exploration, "reviewed", actor)
    session.flush()


def _was_reviewed(session: Session, exploration: Exploration) -> bool:
    return bool(
        session.execute(
            select(
                exists().where(
                    ExplorationEvent.exploration_id == exploration.id,
                    ExplorationEvent.event == "reviewed",
                )
            )
        ).scalar()
    )


def decide(
    session: Session, exploration: Exploration, decision: str, text: str, actor: Actor
) -> None:
    """The author's decision, from a user session and never with an API key.

    Accepting means the measure and its reading hold, not that the hypothesis is
    true: an accepted "against" is a result. It needs the reviewer's event first.
    """
    _require_state(exploration, "reported")
    if actor.type != "user":
        raise ExplorationError("a decision is taken from a user session, never with an API key")
    if decision not in DECISIONS:
        raise ExplorationError(f"decision must be one of {DECISIONS}")
    if not text.strip():
        raise ExplorationError("a decision says why")
    if not _was_reviewed(session, exploration):
        raise ExplorationConflict("the exploration has not been reviewed yet")
    # Read before the move: the query autoflushes, and a decided row without its
    # decision is exactly what the table's CHECK refuses.
    decided_report = _last_report_sha(session, exploration)
    _move(exploration, "decided")
    exploration.decision = decision
    if decided_report is not None:
        # A decision fixes the report it was taken on, by its fingerprint, and
        # it does so whichever way the decision went. A rejection that does not
        # say WHICH report it rejected cannot be checked against anything: the
        # report is rebuilt from rows that keep moving, so the next build of it
        # is a different document and nothing records that the rejection was
        # not about that one.
        exploration.report_sha256 = decided_report
    exploration.decision_text = text.strip()
    exploration.decided_at = func.now()  # type: ignore[assignment]
    exploration.decided_by = actor.name
    _event(session, exploration, "decision", actor, {"decision": decision})
    session.flush()


def archive(session: Session, exploration: Exploration, reason: str, actor: Actor) -> None:
    """Mark the exploration archived with a reason. It is never deleted."""
    if reason not in ARCHIVE_REASONS:
        raise ExplorationError(f"reason must be one of {ARCHIVE_REASONS}")
    if exploration.archived_reason is not None:
        raise ExplorationConflict(f"exploration {exploration.number} is already archived")
    exploration.archived_reason = reason
    exploration.archived_at = func.now()  # type: ignore[assignment]
    _event(session, exploration, "archived", actor, {"reason": reason})
    session.flush()


_HEADLINE = (
    "number",
    "title",
    "question",
    "kind",
    "state",
    "archived_reason",
    "grade",
    "verdict",
    "post_hoc",
    "proposer",
    "launcher",
    "reviewer",
    "decision",
)


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _headline(row: Exploration) -> dict[str, Any]:
    out = {k: getattr(row, k) for k in _HEADLINE}
    out.update(
        id=str(row.id),
        evaluation_set_id=str(row.evaluation_set_id),
        created_at=_iso(row.created_at),
    )
    return out


def describe(session: Session, row: Exploration) -> dict[str, Any]:
    """One exploration with its rules, its links and its log, oldest event first."""
    session.refresh(row)
    links = session.execute(
        select(ExplorationLink)
        .where(ExplorationLink.exploration_id == row.id)
        .order_by(ExplorationLink.created_at, ExplorationLink.id)
    ).scalars()
    events = session.execute(
        select(ExplorationEvent)
        .where(ExplorationEvent.exploration_id == row.id)
        .order_by(ExplorationEvent.id)
    ).scalars()
    return {
        **_headline(row),
        "question": row.question,
        "hypothesis": row.hypothesis,
        "rules": row.rules,
        "rules_sha256": row.rules_sha256,
        "rules_frozen_at": _iso(row.rules_frozen_at),
        "rules_frozen_by": row.rules_frozen_by,
        "decision_text": row.decision_text,
        "decided_at": _iso(row.decided_at),
        "decided_by": row.decided_by,
        "archived_at": _iso(row.archived_at),
        "report_sha256": row.report_sha256,
        "links": [
            {
                "id": str(lk.id),
                "role": lk.role,
                "arm": lk.arm,
                "owned": lk.owned,
                "expires_at": _iso(lk.expires_at),
                "target": {
                    k: str(getattr(lk, k)) for k in LINK_TARGETS if getattr(lk, k) is not None
                },
            }
            for lk in links
        ],
        "events": [
            {
                "at": _iso(ev.at),
                "actor": ev.actor,
                "actor_type": ev.actor_type,
                "event": ev.event,
                "body": ev.body,
                "link_id": str(ev.link_id) if ev.link_id else None,
            }
            for ev in events
        ],
    }


def listing(
    session: Session,
    *,
    state: str | None = None,
    evaluation_set_id: uuid.UUID | None = None,
    include_archived: bool = False,
) -> list[dict[str, Any]]:
    """Explorations by number. Never sorted by a figure."""
    query = select(Exploration).order_by(Exploration.number)
    if state is not None:
        query = query.where(Exploration.state == state)
    if evaluation_set_id is not None:
        query = query.where(Exploration.evaluation_set_id == evaluation_set_id)
    if not include_archived:
        query = query.where(Exploration.archived_reason.is_(None))
    return [_headline(row) for row in session.execute(query).scalars()]
