"""Prompts reutilizables que precargan contexto relevante para Claude."""

from __future__ import annotations

from protea_mcp.server import mcp


@mcp.prompt()
def debug_failed_prediction_set(prediction_set_id: str) -> list[dict]:
    """Diagnóstico de un prediction_set fallido: params + logs + últimas ops."""
    raise NotImplementedError


@mcp.prompt()
def triage_stalled_queue(queue: str) -> list[dict]:
    """Triage de cola stalled: lista jobs, heartbeats de workers, último reaper run."""
    raise NotImplementedError


@mcp.prompt()
def handoff_between_sessions(channel: str) -> list[dict]:
    """Resumen de lo hecho por otras sesiones en un canal — para retomar trabajo."""
    raise NotImplementedError
