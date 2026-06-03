"""Guard against the *_pg integration suite wiping a real database.

The session-scoped ``postgres_url`` fixture in ``tests/conftest.py`` yields
whatever DB the ``PROTEA_PG_*`` env vars point at when all four are set
(the ``external_db`` branch). The ``*_pg.py`` tests then run
``Base.metadata.drop_all(); create_all()`` against it. If those env vars
happen to point at the live dev/prod Postgres (e.g. inherited from a
sourced ``.env``), the schema is destroyed silently. ``_guard_external_db``
default-denies that case unless ``PROTEA_ALLOW_DESTRUCTIVE_TESTS=1`` is set.
"""

from __future__ import annotations

import pytest

from tests.conftest import _guard_external_db


def _call(monkeypatch, *, user="u", host="localhost", port="5544", db="throwaway"):
    return _guard_external_db(user=user, host=host, port=port, db=db)


class TestConftestDbGuard:
    def test_disposable_db_allowed(self, monkeypatch):
        monkeypatch.delenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", raising=False)
        # A random throwaway DB on a non-dev port must pass cleanly.
        _call(monkeypatch, db="protea_pgtest_abc123", port="55321")

    def test_protected_db_name_aborts(self, monkeypatch):
        monkeypatch.delenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="real database"):
            _call(monkeypatch, db="protea", port="55321")

    def test_protected_db_name_case_insensitive(self, monkeypatch):
        monkeypatch.delenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="known dev/prod store"):
            _call(monkeypatch, db="BioData", port="55321")

    def test_dev_host_port_aborts(self, monkeypatch):
        monkeypatch.delenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="dev Postgres"):
            _call(monkeypatch, db="throwaway", host="localhost", port="5432")

    def test_dev_host_port_loopback_alias_aborts(self, monkeypatch):
        monkeypatch.delenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", raising=False)
        with pytest.raises(pytest.fail.Exception, match="dev Postgres"):
            _call(monkeypatch, db="throwaway", host="127.0.0.1", port="5432")

    def test_sentinel_overrides_guard(self, monkeypatch):
        monkeypatch.setenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS", "1")
        # Even the live DB name on the dev host:port is allowed with opt-in.
        _call(monkeypatch, db="protea", host="localhost", port="5432")
