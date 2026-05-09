from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field, field_validator

from protea.api.deps import get_session_factory
from protea.config.tuning import get_tuning
from protea.infrastructure.orm.models.support_entry import SupportEntry
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/support", tags=["support"])


class SupportCreate(BaseModel):
    """Body for ``POST /support``.

    A thumbs-up may carry an optional free-form ``comment``. The text is
    capped at ``api.max_comment_length`` from the tuning config; longer
    submissions are rejected with 422 rather than silently truncated.
    """

    comment: str | None = Field(
        default=None,
        description=(
            "Optional free-form note shown on the support feed. "
            "Capped at ``api.max_comment_length`` from the tuning "
            "config."
        ),
    )

    @field_validator("comment")
    @classmethod
    def comment_within_limit(cls, v: str | None) -> str | None:
        if v is None:
            return v
        max_len = get_tuning().api.max_comment_length
        if len(v) > max_len:
            raise ValueError(f"comment exceeds max length {max_len}")
        return v


@router.get("")
def get_support(
    all_comments: bool = Query(False),
    factory=Depends(get_session_factory),
) -> dict[str, Any]:
    """Return total thumbs-up count and comments.

    Pass ``all_comments=true`` to get all comments (up to the configured
    page limit) instead of the recent_limit most recent.
    """
    api_limits = get_tuning().api
    with session_scope(factory) as session:
        total = session.query(SupportEntry).count()
        limit = api_limits.page_limit if all_comments else api_limits.recent_limit
        recent = (
            session.query(SupportEntry)
            .filter(SupportEntry.comment.isnot(None))
            .order_by(SupportEntry.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "count": total,
            "comments": [
                {"id": str(e.id), "comment": e.comment, "created_at": e.created_at.isoformat()}
                for e in recent
            ],
        }


@router.post("", status_code=201)
def post_support(
    body: SupportCreate,
    factory=Depends(get_session_factory),
) -> dict[str, Any]:
    """Submit a thumbs-up with an optional comment."""
    comment = body.comment.strip() if body.comment else None
    if comment == "":
        comment = None

    with session_scope(factory) as session:
        entry = SupportEntry(comment=comment)
        session.add(entry)
        session.flush()
        total = session.query(SupportEntry).count()
        return {"count": total, "id": str(entry.id)}
