"""The engine every session is bound to, and the one adapter it needs.

PGVECTOR'S TYPES WERE NEVER REGISTERED. ``pgvector.psycopg.register_vector``
appears nowhere in this repository, so psycopg had no binary adapter for
``vector`` / ``halfvec`` and Postgres fell back to sending them as TEXT: the
literal ``[v1,v2,...]``, which the column type then parsed in Python with
``[float(v) for v in value[1:-1].split(",")]``.

Measured back to back on one machine 2026-10-28, reading 300 rows of the
8192-dimension learned-code bank:

    without register_vector   str          12,74 ms a row
    with register_vector      HalfVector    0,75 ms a row

a factor of 17, and the fetch alone went from 3,589 s to 0,225 s. Both numbers
were taken while a prediction was running, so the RATIO is what they establish
and the absolute figures are not a quiet-machine estimate.

IT IS A TIME FIX AND NOT A MEMORY ONE, which is worth saying because the two
got conflated once already: a row costs 16.985 bytes as text and 16.384 as a
dense float16 array, so nothing meaningful is saved by not parsing it. What
holds a whole bank in memory is a client-side buffered result set, which is a
cursor question and still open.

Values do not move. Verified on five rows of that bank: the ORM typed column
returns ``HalfVector`` either way and the arrays are equal bit for bit, because
the text form is a lossless decimal rendering of the same float16s.
"""

from __future__ import annotations

import logging

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine

from protea.config.tuning import get_tuning
from protea.infrastructure.telemetry import instrument_sqlalchemy_engine

logger = logging.getLogger(__name__)


def _register_vector_adapters(dbapi_connection: object, _record: object = None) -> None:
    """Give one psycopg connection the binary adapters for vector and halfvec.

    A named module-level listener and not a closure, so a caller can ask
    ``event.contains(engine, "connect", _register_vector_adapters)`` and get an
    answer. An anonymous listener is registered and unobservable, which is the
    same defect as a guard with no caller.

    Non-fatal on failure, for the reason the telemetry hook beside it is: an
    engine that refuses to build takes the API and twelve workers with it, and
    the text path still works, slowly.
    """
    try:
        from pgvector.psycopg import register_vector

        register_vector(dbapi_connection)
    except Exception:  # pragma: no cover - driver without the adapters
        logger.warning(
            "pgvector types are not registered on this connection, so vectors "
            "arrive as text and every embedding read parses one float per "
            "dimension in Python: 17x slower, measured.",
            exc_info=True,
        )


def _register_vector_types(engine: Engine) -> None:
    """Attach :func:`_register_vector_adapters` to this engine's connections.

    On ``connect`` rather than once per engine: the adapters live on the
    connection, and a pool that recycles one (``pool_recycle_seconds``) or
    opens an overflow one would otherwise serve text from some connections and
    binary from others -- the same bank arriving in two shapes depending on
    which connection answered.

    Silent for any dialect but postgresql, which is how the test suite runs:
    its guard forces a sqlite URL, where ``register_vector`` raises.
    """
    if engine.dialect.name != "postgresql":
        return
    event.listen(engine, "connect", _register_vector_adapters)


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
    _register_vector_types(engine)
    return engine
