"""DDL idempotente para las tablas propias del MCP (audit log, notes, claims).

Se ejecuta en `__main__` antes de `mcp.run()`. Usa `CREATE TABLE IF NOT EXISTS`
para que sea seguro correr repetidas veces.
"""

from __future__ import annotations

from sqlalchemy import text

from protea_mcp.db import get_session_factory, session_scope

DDL = [
    """
    CREATE TABLE IF NOT EXISTS mcp_audit_log (
        id BIGSERIAL PRIMARY KEY,
        session_id TEXT,
        tool TEXT NOT NULL,
        args JSONB NOT NULL,
        result JSONB,
        success BOOLEAN NOT NULL,
        error TEXT,
        ts TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_mcp_audit_ts ON mcp_audit_log (ts DESC)",
    "CREATE INDEX IF NOT EXISTS ix_mcp_audit_tool_ts ON mcp_audit_log (tool, ts DESC)",
    """
    CREATE TABLE IF NOT EXISTS mcp_note (
        id BIGSERIAL PRIMARY KEY,
        channel TEXT NOT NULL,
        author TEXT NOT NULL,
        body TEXT NOT NULL,
        tags TEXT[] NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_mcp_note_channel_ts ON mcp_note (channel, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS mcp_claim (
        task_key TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        expires_at TIMESTAMPTZ NOT NULL
    )
    """,
]


def bootstrap_mcp_schema() -> None:
    factory = get_session_factory()
    with session_scope(factory) as s:
        for stmt in DDL:
            s.execute(text(stmt))
