"""Log de toda escritura vía MCP. Tabla sugerida:

CREATE TABLE mcp_audit_log (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT,
    tool TEXT NOT NULL,
    args JSONB NOT NULL,
    result JSONB,
    success BOOLEAN NOT NULL,
    error TEXT,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator, Optional

from sqlalchemy import text

from protea_mcp.db import session_scope


def record(
    tool: str,
    args: dict[str, Any],
    *,
    session_id: Optional[str] = None,
    result: Optional[dict[str, Any]] = None,
    success: bool = True,
    error: Optional[str] = None,
) -> None:
    with session_scope() as s:
        s.execute(
            text(
                """
                INSERT INTO mcp_audit_log
                    (session_id, tool, args, result, success, error, ts)
                VALUES (:sid, :tool, :args, :res, :ok, :err, :ts)
                """
            ),
            dict(
                sid=session_id,
                tool=tool,
                args=json.dumps(args, default=str),
                res=json.dumps(result, default=str) if result is not None else None,
                ok=success,
                err=error,
                ts=datetime.utcnow(),
            ),
        )


@contextmanager
def audited(tool: str, args: dict[str, Any], session_id: Optional[str] = None) -> Iterator[None]:
    """Context manager: registra éxito/error automáticamente."""
    try:
        yield
        record(tool, args, session_id=session_id, success=True)
    except Exception as e:
        record(tool, args, session_id=session_id, success=False, error=str(e))
        raise
