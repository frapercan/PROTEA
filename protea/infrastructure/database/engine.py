from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from protea.config.tuning import get_tuning
from protea.infrastructure.telemetry import instrument_sqlalchemy_engine


def build_engine(db_url: str) -> Engine:
    settings = get_tuning().worker
    engine = create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        pool_recycle=settings.db_pool_recycle_seconds,
    )
    # T5.1b: attach the SQLAlchemy OTel instrumentor when tracing is
    # enabled. The helper short-circuits when the SDK or instrumentor
    # package is missing so the worker / API boot stays green on
    # minimal images.
    instrument_sqlalchemy_engine(engine)
    return engine
