"""Annotations service — pure-logic helpers extracted from
``protea.api.routers.annotations``.

ORM ↔ dict serialisers and the read-side handlers (snapshot/IA-url
operations) live here so non-router callers (CLI tools, batch
scripts) can reuse them without pulling FastAPI in.

The router translates the domain exceptions raised here to HTTP
responses:

- :class:`EntityNotFoundError` → ``404 Not Found`` (e.g. an
  ``OntologySnapshot`` or ``AnnotationSet`` UUID does not resolve).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


class AnnotationsServiceError(Exception):
    """Base class for annotations-service domain errors."""


class EntityNotFoundError(AnnotationsServiceError):
    """Generic 404 — a referenced entity does not exist.

    Pickle-safe via ``__reduce__`` so the structured ``entity`` /
    ``entity_id`` attrs survive a round-trip without tripping
    flake8-bugbear B042.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:  # noqa: B042
        super().__init__(f"{entity} not found")
        self.entity = entity
        self.entity_id = entity_id

    def __reduce__(self) -> tuple[type, tuple[str, uuid.UUID]]:
        return (self.__class__, (self.entity, self.entity_id))


def snapshot_to_dict(s: OntologySnapshot, term_count: int) -> dict[str, Any]:
    """Serialise an :class:`OntologySnapshot` to its API dict shape."""
    return {
        "id": str(s.id),
        "obo_url": s.obo_url,
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
        "loaded_at": s.loaded_at.isoformat(),
        "go_term_count": term_count,
    }


def list_snapshots_data(session: Session) -> list[dict[str, Any]]:
    """Return all loaded snapshots with their GO term counts (newest first).

    Pure read; the caller is responsible for caching at the API
    boundary if desired (the GROUP BY over the multi-million row
    ``go_term`` table is the slow part).
    """
    count_sub = (
        session.query(
            GOTerm.ontology_snapshot_id,
            func.count(GOTerm.id).label("cnt"),
        )
        .group_by(GOTerm.ontology_snapshot_id)
        .subquery()
    )
    rows = (
        session.query(OntologySnapshot, count_sub.c.cnt)
        .outerjoin(count_sub, OntologySnapshot.id == count_sub.c.ontology_snapshot_id)
        .order_by(OntologySnapshot.loaded_at.desc())
        .all()
    )
    return [snapshot_to_dict(s, cnt or 0) for s, cnt in rows]


def get_snapshot_data(
    session: Session,
    snapshot_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single snapshot with its GO term count.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    term_count = (
        session.query(func.count(GOTerm.id))
        .filter(GOTerm.ontology_snapshot_id == snapshot_id)
        .scalar()
    )
    return snapshot_to_dict(s, term_count or 0)


def set_snapshot_ia_url(
    session: Session,
    snapshot_id: uuid.UUID,
    ia_url: str | None,
) -> dict[str, Any]:
    """Update the IA URL on a snapshot. Empty string is treated as ``None``.

    Returns a small confirmation dict shape compatible with the
    legacy endpoint. Raises :class:`EntityNotFoundError` for the
    404 path. The caller (router) is responsible for validating
    request body shape (e.g. presence of the ``ia_url`` key) before
    calling.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    s.ia_url = ia_url or None
    session.flush()
    return {
        "id": str(s.id),
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
    }


__all__ = [
    "AnnotationsServiceError",
    "EntityNotFoundError",
    "get_snapshot_data",
    "list_snapshots_data",
    "set_snapshot_ia_url",
    "snapshot_to_dict",
]
