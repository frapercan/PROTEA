from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def build_engine(db_url: str) -> Engine:
    return create_engine(db_url, future=True, pool_pre_ping=True)
