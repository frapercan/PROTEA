"""pgvector's psycopg adapters were never registered, so vectors came as TEXT.

``register_vector`` appears nowhere in the repository's history, so Postgres
fell back to sending ``vector`` and ``halfvec`` as the literal ``[v1,v2,...]``
and the column type parsed it in Python, one ``float()`` per dimension.

MEASURED back to back on one machine 2026-10-28, 300 rows of the
8192-dimension learned-code bank:

    without register_vector   str          12,74 ms a row, fetch 3,589 s
    with register_vector      HalfVector    0,75 ms a row, fetch 0,225 s

a factor of 17. Both were taken while a prediction was running, so what they
establish is the RATIO.

A TIME FIX AND NOT A MEMORY ONE. A row is 16.985 bytes as text and 16.384 as a
dense float16 array, so not parsing it saves nothing worth naming. What holds a
whole bank resident is a client-side buffered result set, a cursor question that
this change does not touch.

Values do not move: verified on five rows of that bank, the ORM typed column
returns ``HalfVector`` either way and the arrays are equal bit for bit.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine, event

from protea.infrastructure.database.engine import (
    _register_vector_adapters,
    _register_vector_types,
)


def _pg_engine():
    """A postgresql engine that has not connected to anything.

    ``create_engine`` is lazy, so this needs no server and the listener can be
    inspected without one.
    """
    return create_engine("postgresql+psycopg://user:pw@127.0.0.1:1/none")


def test_a_postgres_engine_gets_a_connect_listener():
    engine = _pg_engine()
    assert not event.contains(engine, "connect", _register_vector_adapters)
    _register_vector_types(engine)
    assert event.contains(engine, "connect", _register_vector_adapters)


def test_sqlite_is_left_alone():
    """The suite's own database.

    ``conftest`` forces a sqlite URL and ``register_vector`` raises on a driver
    that has no pgvector adapters, so the guard is what keeps the whole suite
    from failing at engine construction rather than a tidiness.
    """
    engine = create_engine("sqlite:///:memory:")
    _register_vector_types(engine)
    assert not event.contains(engine, "connect", _register_vector_adapters)


def test_the_listener_is_per_connection_and_not_per_engine():
    """Why ``connect`` and not a one-off call.

    The adapters live on the connection. A pool that recycles one, or opens an
    overflow one, would otherwise serve text from some connections and binary
    from others: the same bank arriving in two shapes depending on which
    connection answered.
    """
    import inspect

    source = inspect.getsource(_register_vector_types)
    assert 'event.listen(engine, "connect"' in source
    # And not one of the engine-wide hooks, which fire once and would leave
    # every later connection on the text path.
    for once_only in ("first_connect", "engine_connect"):
        assert once_only not in source


def test_a_driver_without_the_adapters_warns_and_does_not_raise(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """An engine that refuses to build takes the API and twelve workers with it.

    The text path still works, slowly, so a missing adapter is a warning and
    not a boot failure. Same reasoning as the telemetry hook beside it.
    """
    import builtins

    real_import = builtins.__import__

    def _no_pgvector(name, *args, **kwargs):
        if name == "pgvector.psycopg":
            raise ImportError("no pgvector here")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_pgvector)
    with caplog.at_level(logging.WARNING):
        _register_vector_adapters(object())
    assert any("arrive as text" in r.message for r in caplog.records)


def test_the_warning_names_the_measured_cost():
    """A warning nobody can act on gets ignored. This one says what it costs."""
    import inspect

    source = inspect.getsource(_register_vector_adapters)
    assert "17x" in source
