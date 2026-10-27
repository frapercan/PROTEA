"""Deleting an annotation set must drop the list cache that still shows it.

``GET /v1/annotations/sets`` is cached for five minutes because its GROUP BY over
``protein_go_annotation`` takes six seconds. Nothing dropped that entry on a
delete, so the list kept serving sets that no longer existed. Measured
2026-10-05: after deleting all 71 carried-over sets, the database answered 0 and
the API kept answering 71 -- and because the read uses
``serve_stale_on_error=True``, a database blip inside the window would have
extended the stale answer rather than ending it.

The per-source views are cached under their own keys, so both have to go.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.cache import cached
from protea.api.cache import invalidate as _cache_invalidate
from protea.api.routers.annotations.sets import (
    _annotation_sets_cache_key,
    router,
)


@pytest.fixture(autouse=True)
def _reset_cache():
    _cache_invalidate()
    yield
    _cache_invalidate()


@contextmanager
def _mock_scope(session):
    yield session


def _client(session):
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(router)
    return TestClient(app)


def _seed_both_views():
    """Put a known payload in the unfiltered and the ``goa`` view."""
    for source in (None, "goa"):
        cached(_annotation_sets_cache_key(source), 300.0, lambda: [{"stale": True}])


class TestTheDeleteDropsTheListCache:
    def test_both_views_are_dropped(self):
        session = MagicMock()
        _seed_both_views()
        assert cached(_annotation_sets_cache_key(None), 300.0, list) == [{"stale": True}]

        with (
            patch("protea.api.routers.annotations.sets.session_scope", lambda f: _mock_scope(session)),
            patch(
                "protea.api.routers.annotations.sets.delete_annotation_set_data",
                return_value={"deleted": "x", "source": "goa", "annotations_deleted": 0},
            ),
        ):
            r = _client(session).delete("/sets/0f8b1c3e-0000-4000-8000-000000000001")

        assert r.status_code == 200
        # A miss recomputes, so an empty producer now wins: the stale entry is gone.
        assert cached(_annotation_sets_cache_key(None), 300.0, list) == []
        assert cached(_annotation_sets_cache_key("goa"), 300.0, list) == []

    def test_the_summary_carries_the_source(self):
        """The router needs it to pick the per-source key, so a service that
        stopped returning it would silently leave that view stale."""
        session = MagicMock()
        with (
            patch("protea.api.routers.annotations.sets.session_scope", lambda f: _mock_scope(session)),
            patch(
                "protea.api.routers.annotations.sets.delete_annotation_set_data",
                return_value={"deleted": "x", "source": "quickgo", "annotations_deleted": 7},
            ),
        ):
            r = _client(session).delete("/sets/0f8b1c3e-0000-4000-8000-000000000002")
        assert r.json()["source"] == "quickgo"
        assert r.json()["annotations_deleted"] == 7
