from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from protea.config.tuning import get_tuning


def build_engine(db_url: str) -> Engine:
    settings = get_tuning().worker
    return create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        pool_recycle=settings.db_pool_recycle_seconds,
    )
