"""Evaluated explorations over HTTP (ADR-D55).

Reads are public, like the other registries. Every write is for operators,
except the decision, which is the author's alone and is refused to an API key
whatever role it carries: an API key passes the same role check as a person,
so the role cannot be what tells them apart.

The logic lives in :mod:`protea.services.explorations_service`; this module
maps its errors to status codes: a request its rules refuse is 422, an unknown
exploration 404, a move its state does not allow 409.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, sessionmaker

from protea.api.bearer import BearerPrincipal
from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.infrastructure.session import session_scope
from protea.services import explorations_service as svc

router = APIRouter(prefix="/explorations", tags=["explorations"])


class ExplorationCreateRequest(BaseModel):
    """A hypothesis, the one window it is answered on, and its draft rules."""

    title: str = Field(description="What a person calls it, in a line.")
    question: str = Field(description="The question it answers.")
    hypothesis: str = Field(description="The falsifiable claim, stated before any number.")
    kind: str = Field(description="one_axis, full_method or instrument.")
    evaluation_set_id: uuid.UUID = Field(description="The evaluation window.")
    rules: dict[str, Any] = Field(default_factory=dict, description="The draft rules.")


class ExplorationRulesRequest(BaseModel):
    """A draft's rules, replaced whole."""

    rules: dict[str, Any] = Field(description="The rules, whole.")


class ExplorationAmendRequest(BaseModel):
    """Frozen rules changed after launch, with the reason; the log keeps both fingerprints."""

    rules: dict[str, Any] = Field(description="The amended rules, whole.")
    reason: str = Field(description="Why they change after being frozen.")


class ExplorationLinkRequest(BaseModel):
    """One object the exploration uses or produces: exactly one target."""

    role: str = Field(
        description="input, arm, result, strata, comparison, job, temporary, reference, "
        "supports or supersedes."
    )
    target: dict[str, Any] = Field(
        description="Exactly one target, e.g. {'prediction_set_id': '...'}; a store key "
        "travels with its sha256."
    )
    arm: str | None = Field(default=None, description="The arm's name, for an arm's link.")
    owned: bool = Field(default=False, description="Whether the exploration created the target.")
    expires_at: datetime | None = Field(
        default=None, description="When the target may be collected; mandatory for temporary."
    )
    size_bytes: int | None = Field(default=None, ge=0, description="The target's footprint.")


class ExplorationEventRequest(BaseModel):
    """An objection, a reply or an answered check, on a reported exploration."""

    event: str = Field(description="objection, reply or check_answered.")
    body: dict[str, Any] = Field(
        default_factory=dict, description="What is said: the text, the cell, the answer."
    )
    link_id: uuid.UUID | None = Field(default=None, description="The link it is about, if any.")


class ExplorationDecisionRequest(BaseModel):
    """The author's decision on a reviewed exploration."""

    decision: str = Field(description="accepted or rejected.")
    text: str = Field(description="Why, in the author's words.")


class ExplorationArchiveRequest(BaseModel):
    """Archiving, with its reason. Nothing is deleted."""

    reason: str = Field(description="abandoned, closed, superseded or withdrawn.")


def actor_of(principal: Any) -> svc.Actor:
    """The principal as an actor: a user session, an API key, or the local gate."""
    if isinstance(principal, BearerPrincipal):
        name = principal.claims.get("email") or principal.sub
        return svc.Actor(name=str(name), type="user")
    if principal is None:
        # The gate is disabled, which only a development stack does.
        return svc.Actor(name="local", type="user")
    return svc.Actor(name=f"key:{getattr(principal, 'name', 'unnamed')}", type="api_key")


def _http(exc: Exception) -> HTTPException:
    if isinstance(exc, svc.ExplorationNotFound):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, svc.ExplorationConflict):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=422, detail=str(exc))


_ERRORS = (svc.ExplorationError, svc.ExplorationNotFound, svc.ExplorationConflict)


def _write(factory: sessionmaker[Session], ref: str | None, principal: Any, action: Any) -> Any:
    """Run one move in one transaction and answer the exploration as it now stands."""
    try:
        with session_scope(factory) as session:
            actor = actor_of(principal)
            row = action(session, svc.find(session, ref) if ref else None, actor)
            return svc.describe(session, row)
    except _ERRORS as exc:
        raise _http(exc) from exc


@router.get("", summary="List explorations, by number")
def list_explorations(
    state: str | None = Query(
        default=None, description="Only explorations in this state: draft, running, ..."
    ),
    evaluation_set_id: uuid.UUID | None = Query(
        default=None, description="Only explorations answered on this window."
    ),
    include_archived: bool = Query(
        default=False, description="Include archived explorations, which are hidden by default."
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Explorations by number, never sorted by a figure."""
    with session_scope(factory) as session:
        return svc.listing(
            session,
            state=state,
            evaluation_set_id=evaluation_set_id,
            include_archived=include_archived,
        )


@router.get("/{ref}", summary="One exploration with its rules, links and log")
def get_exploration(
    ref: str, factory: sessionmaker[Session] = Depends(get_session_factory)
) -> dict[str, Any]:
    """One exploration, by id or number, with its links and its log, oldest event first."""
    try:
        with session_scope(factory) as session:
            return svc.describe(session, svc.find(session, ref))
    except _ERRORS as exc:
        raise _http(exc) from exc


@router.get("/{ref}/receipt", summary="The receipt, computed from rows")
def get_receipt(
    ref: str, factory: sessionmaker[Session] = Depends(get_session_factory)
) -> dict[str, Any]:
    """The receipt as the rows stand now: each layer, or "not recorded", and what blocks.

    Computed on every read and never stored by hand: the method seal between the
    two arms, their recipes and whether each id rederives, the frame of every
    linked result, and the scale its scores were read in.
    """
    from protea.services.exploration_receipt import compute

    try:
        with session_scope(factory) as session:
            return compute(session, svc.find(session, ref))
    except _ERRORS as exc:
        raise _http(exc) from exc


@router.post("", status_code=201, summary="Create a draft exploration")
def create_exploration(
    body: ExplorationCreateRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """A draft, numbered after the last one; the caller is its proposer."""
    return _write(
        factory,
        None,
        principal,
        lambda s, _row, actor: svc.create(s, svc.Draft(**body.model_dump()), actor),
    )


@router.put("/{ref}/rules", summary="Replace a draft's rules")
def edit_rules(
    ref: str,
    body: ExplorationRulesRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Replace a draft's rules whole. Once launched, only an amendment changes them."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.edit_rules(s, row, body.rules, actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/launch", summary="Freeze the rules and start running")
def launch_exploration(
    ref: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Freeze the rules with their fingerprint and the database time.

    The launcher must be a principal other than the proposer. The exploration is
    marked post hoc when an arm already had results in its window.
    """

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.launch(s, row, actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/amendments", summary="Amend frozen rules, with a reason")
def amend_rules(
    ref: str,
    body: ExplorationAmendRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Change frozen rules; the log records both fingerprints and the reason."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.amend(s, row, body.rules, body.reason, actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/links", status_code=201, summary="Link one object to the exploration")
def link_object(
    ref: str,
    body: ExplorationLinkRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Link one object to a draft or running exploration; the target cannot then be deleted."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.link(s, row, svc.LinkSpec(**body.model_dump()), actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/events", status_code=201, summary="An objection, a reply or a check")
def append_event(
    ref: str,
    body: ExplorationEventRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Append an objection, a reply or an answered check to a reported exploration's log."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.append(s, row, event=body.event, body=body.body, actor=actor, link_id=body.link_id)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/review", summary="The reviewer's mark, which the decision requires")
def review_exploration(
    ref: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Mark a reported exploration reviewed; its proposer cannot."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.review(s, row, actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/decision", summary="The author's decision, from a user session only")
def decide_exploration(
    ref: str,
    body: ExplorationDecisionRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Accept or reject a reviewed exploration. Refused to an API key, whatever its role."""
    if not isinstance(principal, BearerPrincipal):
        raise HTTPException(
            status_code=403,
            detail="a decision is taken from the author's user session, never with an API key",
        )

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.decide(s, row, body.decision, body.text, actor)
        return row

    return _write(factory, ref, principal, act)


@router.post("/{ref}/archive", summary="Archive with a reason; nothing is deleted")
def archive_exploration(
    ref: str,
    body: ExplorationArchiveRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Archive an exploration with its reason. It stays readable; superseded ones struck through."""

    def act(s: Session, row: Any, actor: svc.Actor) -> Any:
        svc.archive(s, row, body.reason, actor)
        return row

    return _write(factory, ref, principal, act)
