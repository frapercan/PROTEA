"""Buzón inter-sesiones Claude + locks cooperativos sobre mcp_note/mcp_claim."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from protea_mcp.db import get_session_factory, session_scope
from protea_mcp.schemas import ClaimRequest, NotePost
from protea_mcp.server import mcp


@mcp.tool()
def post_note(note: NotePost) -> dict:
    """Publica un mensaje en un canal."""
    with session_scope(get_session_factory()) as s:
        row = s.execute(
            text(
                """
                INSERT INTO mcp_note (channel, author, body, tags)
                VALUES (:channel, :author, :body, :tags)
                RETURNING id, created_at
                """
            ),
            dict(
                channel=note.channel,
                author=note.author,
                body=note.body,
                tags=note.tags,
            ),
        ).mappings().one()
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


@mcp.tool()
def read_notes(
    channel: str,
    since: Optional[datetime] = None,
    limit: int = 50,
    tag: Optional[str] = None,
) -> list[dict]:
    """Lee mensajes del canal (opcionalmente filtrados por fecha o tag)."""
    sql = """
        SELECT id, channel, author, body, tags, created_at
        FROM mcp_note
        WHERE channel = :channel
          AND (:since::timestamptz IS NULL OR created_at > :since)
          AND (:tag::text IS NULL OR :tag = ANY(tags))
        ORDER BY created_at DESC
        LIMIT :limit
    """
    with session_scope(get_session_factory()) as s:
        rows = s.execute(
            text(sql),
            dict(channel=channel, since=since, tag=tag, limit=limit),
        ).mappings().all()
    return [
        {
            "id": r["id"],
            "channel": r["channel"],
            "author": r["author"],
            "body": r["body"],
            "tags": list(r["tags"] or []),
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]


@mcp.tool()
def list_channels() -> list[dict]:
    """Canales activos con último mensaje y conteo."""
    with session_scope(get_session_factory()) as s:
        rows = s.execute(
            text(
                """
                SELECT channel,
                       COUNT(*) AS n_notes,
                       MAX(created_at) AS last_at
                FROM mcp_note
                GROUP BY channel
                ORDER BY last_at DESC
                """
            )
        ).mappings().all()
    return [
        {
            "channel": r["channel"],
            "n_notes": r["n_notes"],
            "last_at": r["last_at"].isoformat() if r["last_at"] else None,
        }
        for r in rows
    ]


@mcp.tool()
def claim_task(req: ClaimRequest) -> dict:
    """Lock cooperativo con TTL. Falla si ya lo tiene otra sesión y no ha expirado."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=req.ttl_seconds)
    with session_scope(get_session_factory()) as s:
        existing = s.execute(
            text("SELECT session_id, expires_at FROM mcp_claim WHERE task_key = :k"),
            dict(k=req.task_key),
        ).mappings().first()
        if existing and existing["expires_at"] > now and existing["session_id"] != req.session_id:
            return {
                "claimed": False,
                "held_by": existing["session_id"],
                "expires_at": existing["expires_at"].isoformat(),
            }
        s.execute(
            text(
                """
                INSERT INTO mcp_claim (task_key, session_id, claimed_at, expires_at)
                VALUES (:k, :sid, :now, :exp)
                ON CONFLICT (task_key) DO UPDATE
                SET session_id = EXCLUDED.session_id,
                    claimed_at = EXCLUDED.claimed_at,
                    expires_at = EXCLUDED.expires_at
                """
            ),
            dict(k=req.task_key, sid=req.session_id, now=now, exp=expires),
        )
    return {"claimed": True, "session_id": req.session_id, "expires_at": expires.isoformat()}


@mcp.tool()
def release_claim(task_key: str, session_id: str) -> dict:
    """Libera un claim — solo el dueño actual."""
    with session_scope(get_session_factory()) as s:
        res = s.execute(
            text(
                "DELETE FROM mcp_claim WHERE task_key = :k AND session_id = :sid"
            ),
            dict(k=task_key, sid=session_id),
        )
    return {"released": res.rowcount > 0}


@mcp.tool()
def list_claims(active_only: bool = True) -> list[dict]:
    """Claims vivos (o todos si active_only=False)."""
    sql = "SELECT task_key, session_id, claimed_at, expires_at FROM mcp_claim"
    if active_only:
        sql += " WHERE expires_at > now()"
    sql += " ORDER BY claimed_at DESC"
    with session_scope(get_session_factory()) as s:
        rows = s.execute(text(sql)).mappings().all()
    return [
        {
            "task_key": r["task_key"],
            "session_id": r["session_id"],
            "claimed_at": r["claimed_at"].isoformat(),
            "expires_at": r["expires_at"].isoformat(),
        }
        for r in rows
    ]
