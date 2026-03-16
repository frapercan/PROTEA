from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from starlette.requests import Request

from protea.infrastructure.orm.models.support_entry import SupportEntry
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/support", tags=["support"])

_MAX_COMMENT_LENGTH = 500
_RECENT_LIMIT = 20
_PAGE_LIMIT = 100


def get_session_factory(request: Request):
    return request.app.state.session_factory


class SupportCreate(BaseModel):
    comment: str | None = Field(default=None, max_length=_MAX_COMMENT_LENGTH)


@router.get("")
def get_support(
    all_comments: bool = Query(False),
    factory=Depends(get_session_factory),
) -> dict[str, Any]:
    """Return total thumbs-up count and comments.

    Pass ``all_comments=true`` to get all comments (up to 100) instead of the 20 most recent.
    """
    with session_scope(factory) as session:
        total = session.query(SupportEntry).count()
        limit = _PAGE_LIMIT if all_comments else _RECENT_LIMIT
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
