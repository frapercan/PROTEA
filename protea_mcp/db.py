"""Session factory compartido con PROTEA. Reusa `build_session_factory` y
`load_settings` para que el MCP lea exactamente el mismo postgres que los
workers/API."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import Settings, load_settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings(PROJECT_ROOT)


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return build_session_factory(get_settings().db_url)


__all__ = ["get_settings", "get_session_factory", "session_scope"]
