from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy.orm import Session, sessionmaker

from protea.infrastructure.database.engine import build_engine


def build_session_factory(db_url: str) -> sessionmaker[Session]:
    engine = build_engine(db_url)
    return sessionmaker(bind=engine, class_=Session, autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
